// Package pipeline 提供批次处理：解析、归一化、去重、来源冲突判定、乱序取舍、提交/回滚。
//
// 确定性规则：
//  1. 批次原子性：任一条记录被拒绝（解析失败、身份冲突、同号事件冲突）则整批回滚，
//     身份登记与已提交结论均保持不变；修复后重放与一次性成功结果完全一致。
//  2. 去重：按 (来源系统, EventID) 去重。事件编号的作用域是来源系统：
//     不同来源系统的同一编号是两个不同事件，不会互相顶掉。
//     同来源同号且内容完全一致是幂等重复投递（Outcome.Duplicate=true）；
//     同来源同号但患者/时间/编码/取值等对不上、无法判定是否同一件事时，
//     明确拒绝（EVENT_CONFLICT），绝不任选其一。
//  3. 乱序取舍：同一状态键 (患者, 事件类型, 编码) 下，
//     按 (OccurredAt, EventID) 字典序取最大者，与到达顺序无关。
//  4. 资源有界：仅保存去重集合与每个状态键的最新事件，无无界缓冲。
//  5. 持久化：注入 Persister 后，每个成功批次在内存状态变更被承认前先做一次
//     原子快照落盘；落盘失败则整批内存变更回滚，保证磁盘与内存、跨进程结论一致。
package pipeline

import (
	"fmt"
	"sort"
	"sync"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/identity"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/source"
)

// Message 是一条待处理输入。
type Message struct {
	Source       model.SourceType
	SourceSystem string // 来源系统标识（为空则由解析器默认值或消息内容决定）
	Body         string // 文本消息体或单个 JSON 对象字符串
}

// seenKey 构造去重键：事件编号作用域为来源系统。
func seenKey(system, eventID string) string {
	return system + "\x00" + eventID
}

// Store 是已提交状态：按 (患者, 事件类型, 编码) 保留最新事件，并记录已见事件。
type Store struct {
	mu     sync.Mutex
	seen   map[string]model.NormalizedEvent // (来源系统,EventID) -> 已提交事件（供同号一致性比对）
	latest map[string]model.NormalizedEvent // 状态键 -> 最新事件
}

// NewStore 创建空存储。
func NewStore() *Store {
	return &Store{
		seen:   make(map[string]model.NormalizedEvent),
		latest: make(map[string]model.NormalizedEvent),
	}
}

// stateKey 是乱序取舍的状态键。
func stateKey(ev model.NormalizedEvent) string {
	return ev.CanonicalPID + "|" + ev.EventType + "|" + ev.Code
}

// Latest 返回当前已提交的最新事件快照（键为状态键）。
func (s *Store) Latest() map[string]model.NormalizedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]model.NormalizedEvent, len(s.latest))
	for k, v := range s.latest {
		out[k] = v
	}
	return out
}

// SeenCount 返回已去重的不同 (来源系统,事件编号) 数（用于资源占用观测）。
func (s *Store) SeenCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.seen)
}

// StoreSnapshot 是 Store 的可持久化快照。
type StoreSnapshot struct {
	Seen   []model.NormalizedEvent `json:"seen"`
	Latest []model.NormalizedEvent `json:"latest"`
}

// Snapshot 导出确定性快照（按去重键与状态键排序）。
func (s *Store) Snapshot() StoreSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}

func (s *Store) snapshotLocked() StoreSnapshot {
	seenKeys := make([]string, 0, len(s.seen))
	for k := range s.seen {
		seenKeys = append(seenKeys, k)
	}
	sort.Strings(seenKeys)
	snap := StoreSnapshot{Seen: make([]model.NormalizedEvent, 0, len(seenKeys))}
	for _, k := range seenKeys {
		snap.Seen = append(snap.Seen, s.seen[k])
	}
	latestKeys := make([]string, 0, len(s.latest))
	for k := range s.latest {
		latestKeys = append(latestKeys, k)
	}
	sort.Strings(latestKeys)
	snap.Latest = make([]model.NormalizedEvent, 0, len(latestKeys))
	for _, k := range latestKeys {
		snap.Latest = append(snap.Latest, s.latest[k])
	}
	return snap
}

// LoadSnapshot 从快照重建 Store（仅启动时调用）。
// 校验内部一致性：latest 中的每个事件都必须能在 seen 中找到。
func (s *Store) LoadSnapshot(snap StoreSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := make(map[string]model.NormalizedEvent, len(snap.Seen))
	for _, ev := range snap.Seen {
		if ev.SourceSystem == "" {
			return fmt.Errorf("结论快照损坏: 事件 %s 缺少来源系统", ev.EventID)
		}
		k := seenKey(ev.SourceSystem, ev.EventID)
		if _, dup := seen[k]; dup {
			return fmt.Errorf("结论快照损坏: 去重键 %q 重复", k)
		}
		seen[k] = ev
	}
	latest := make(map[string]model.NormalizedEvent, len(snap.Latest))
	for _, ev := range snap.Latest {
		k := stateKey(ev)
		if _, has := seen[seenKey(ev.SourceSystem, ev.EventID)]; !has {
			return fmt.Errorf("结论快照损坏: 最新结论 %s 不在已见事件集合中", ev.EventID)
		}
		latest[k] = ev
	}
	s.seen = seen
	s.latest = latest
	return nil
}

// stagedEntry 把暂存事件与其输入下标绑定。
type stagedEntry struct {
	ev     model.NormalizedEvent
	msgIdx int
}

// Pipeline 处理批次并提交到 Store。
type Pipeline struct {
	reg       *identity.Registry
	commitMu  sync.Mutex // 串行化批次提交，保证快照与内存状态一一对应
	persister Persister
}

// Persister 在批次提交时原子持久化“注册表 + 结论”的合并快照。
// 返回错误则批次不生效（内存状态不变更）。nil 表示纯内存运行。
type Persister interface {
	Persist(idSnap identity.Snapshot, storeSnap StoreSnapshot) error
}

// Option 配置 Pipeline。
type Option func(*Pipeline)

// WithPersistence 注入持久化钩子。
func WithPersistence(p Persister) Option {
	return func(pl *Pipeline) { pl.persister = p }
}

// New 创建管线。
func New(reg *identity.Registry, opts ...Option) *Pipeline {
	pl := &Pipeline{reg: reg}
	for _, o := range opts {
		o(pl)
	}
	return pl
}

// Result 是批次处理结果。
type Result struct {
	Outcomes   []model.Outcome // 与输入一一对应
	Committed  bool            // 是否已提交
	RolledBack bool            // 是否因部分失败而整批回滚
	Err        error           // 持久化等基础设施错误（此时批次同样未生效）
}

// parse 按来源类型分发解析。
func parse(m Message) (model.RawRecord, *model.Rejection) {
	switch m.Source {
	case model.SourceText:
		return source.ParseText(m.Body, m.SourceSystem)
	case model.SourceJSON:
		return source.ParseJSON([]byte(m.Body), m.SourceSystem)
	default:
		return model.RawRecord{}, &model.Rejection{
			Reason: model.RejectMalformed, Detail: "未知来源类型: " + string(m.Source),
		}
	}
}

// ProcessBatch 处理一批消息，语义见包注释。
func (p *Pipeline) ProcessBatch(store *Store, msgs []Message) Result {
	res := Result{Outcomes: make([]model.Outcome, len(msgs))}

	// 提交串行化：并发批次也按确定顺序提交，快照不串批。
	p.commitMu.Lock()
	defer p.commitMu.Unlock()

	tx := p.reg.Begin()
	staged := make([]stagedEntry, 0, len(msgs))
	failed := false

	// 第一阶段：解析 + 身份暂存（只读写事务视图，不触碰已提交注册表）。
	for i, m := range msgs {
		rec, rej := parse(m)
		if rej == nil {
			canon := identity.Canonicalize(rec.PatientID)
			switch {
			case canon == "":
				rej = &model.Rejection{
					Reason: model.RejectMissingField, Field: "patientId",
					Detail: "患者标识归一化后为空", RawHint: rec.PatientID,
				}
			case rec.SourceSystem == "":
				rej = &model.Rejection{
					Reason: model.RejectMissingField, Field: "sourceSystem",
					Detail: "来源系统缺失，无法确定事件编号作用域", RawHint: rec.PatientID,
				}
			default:
				deid, ok := tx.Resolve(canon, rec.PatientID, rec.Attrs)
				if !ok {
					rej = &model.Rejection{
						Reason: model.RejectIdentityConflict, Field: "patientId",
						Detail: "规范键 " + canon + " 的身份属性与已登记值冲突", RawHint: rec.PatientID,
					}
				} else {
					ev := model.NormalizedEvent{
						EventID:        rec.EventID,
						SourceSystem:   rec.SourceSystem,
						CanonicalPID:   canon,
						DeidentifiedID: deid,
						EventType:      rec.EventType,
						Code:           rec.Code,
						OccurredAt:     rec.OccurredAt,
						Value:          rec.Value,
					}
					res.Outcomes[i] = model.Outcome{
						Event: &ev, Quality: model.Quality{Accepted: true},
					}
					staged = append(staged, stagedEntry{ev: ev, msgIdx: i})
					continue
				}
			}
		}
		res.Outcomes[i] = model.Outcome{Quality: model.Quality{Accepted: false, Rejection: rej}}
		failed = true
	}

	if failed {
		tx.Abort() // 回滚：身份暂存丢弃，store 全程未触碰。
		res.RolledBack = true
		return res
	}

	// 第二阶段：store 锁内完成同号冲突预检（已提交 + 本批内部）与内存变更。
	store.mu.Lock()
	batchSeen := make(map[string]model.NormalizedEvent, len(staged))
	rejectConflict := func(e stagedEntry, cur model.NormalizedEvent) Result {
		store.mu.Unlock()
		tx.Abort()
		res.Outcomes[e.msgIdx] = model.Outcome{Quality: model.Quality{
			Accepted: false,
			Rejection: &model.Rejection{
				Reason: model.RejectEventConflict, Field: "eventId",
				Detail: fmt.Sprintf("来源系统 %s 中事件编号 %s 已存在但内容不一致（已存: code=%s at=%s value=%q；新到: code=%s at=%s value=%q），无法判定是否同一件事",
					e.ev.SourceSystem, e.ev.EventID,
					cur.Code, cur.OccurredAt.Format("2006-01-02T15:04:05Z07:00"), cur.Value,
					e.ev.Code, e.ev.OccurredAt.Format("2006-01-02T15:04:05Z07:00"), e.ev.Value),
				RawHint: e.ev.EventID,
			},
		}}
		res.RolledBack = true
		return res
	}
	for _, e := range staged {
		k := seenKey(e.ev.SourceSystem, e.ev.EventID)
		// 与已提交状态比对。
		if cur, dup := store.seen[k]; dup && !sameEvent(cur, e.ev) {
			return rejectConflict(e, cur)
		}
		// 与本批更早的记录比对：同号不一致 -> 冲突；同号一致 -> 批内重复。
		if cur, dup := batchSeen[k]; dup && !sameEvent(cur, e.ev) {
			return rejectConflict(e, cur)
		}
		batchSeen[k] = e.ev
	}

	dupFlags := make([]bool, len(staged))
	type priorLatest struct {
		ev  model.NormalizedEvent
		had bool
	}
	prior := make(map[string]priorLatest, len(staged))
	addedSet := make(map[string]bool, len(staged))
	for i, e := range staged {
		sk := seenKey(e.ev.SourceSystem, e.ev.EventID)
		if _, dup := store.seen[sk]; dup {
			dupFlags[i] = true // 与已提交内容一致：幂等重复
			continue
		}
		if addedSet[sk] {
			dupFlags[i] = true // 与本批前序记录一致：批内幂等重复
			continue
		}
		store.seen[sk] = e.ev
		addedSet[sk] = true
		lk := stateKey(e.ev)
		if cur, has := store.latest[lk]; has {
			prior[lk] = priorLatest{ev: cur, had: true}
		} else {
			prior[lk] = priorLatest{had: false}
		}
		if cur, has := store.latest[lk]; !has || less(cur, e.ev) {
			store.latest[lk] = e.ev
		}
	}

	var storeSnap StoreSnapshot
	var idSnap identity.Snapshot
	needPersist := p.persister != nil && len(staged) > 0
	if needPersist {
		storeSnap = store.snapshotLocked()
		idSnap = p.reg.SnapshotWithTx(tx)
	}
	store.mu.Unlock()

	// 第三阶段：先原子落盘，再承认提交。落盘失败则回滚本批全部内存变更。
	if needPersist {
		if err := p.persister.Persist(idSnap, storeSnap); err != nil {
			store.mu.Lock()
			for sk := range addedSet {
				delete(store.seen, sk)
			}
			for lk, pl := range prior {
				if pl.had {
					store.latest[lk] = pl.ev
				} else {
					delete(store.latest, lk)
				}
			}
			store.mu.Unlock()
			tx.Abort()
			res.Err = err
			return res
		}
	}
	p.reg.Commit(tx)

	for i, e := range staged {
		if dupFlags[i] {
			res.Outcomes[e.msgIdx].Duplicate = true
		}
	}
	res.Committed = true
	return res
}

// sameEvent 判定同来源同号的两条事件是否同一件事。
// 患者、时间、事件类型、编码、取值任一不一致即视为无法判定的冲突，绝不静默挑一个。
func sameEvent(a, b model.NormalizedEvent) bool {
	return a.SourceSystem == b.SourceSystem &&
		a.EventID == b.EventID &&
		a.CanonicalPID == b.CanonicalPID &&
		a.EventType == b.EventType &&
		a.Code == b.Code &&
		a.Value == b.Value &&
		a.OccurredAt.Equal(b.OccurredAt)
}

// less 定义乱序取舍规则：按 (OccurredAt, EventID) 字典序。
func less(a, b model.NormalizedEvent) bool {
	if !a.OccurredAt.Equal(b.OccurredAt) {
		return a.OccurredAt.Before(b.OccurredAt)
	}
	return a.EventID < b.EventID
}
