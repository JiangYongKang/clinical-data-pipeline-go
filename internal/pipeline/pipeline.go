// Package pipeline 提供批次处理：解析、归一化、去重、乱序取舍、提交/回滚。
//
// 确定性规则：
//  1. 批次原子性：任一条记录被拒绝则整批回滚，已提交状态不变；
//     修复后重放与一次性成功结果一致。
//  2. 去重：按 EventID 去重，首次投递生效，重复投递为幂等空操作。
//  3. 乱序取舍：同一状态键 (患者, 事件类型, 编码) 下，
//     按 (OccurredAt, EventID) 字典序取最大者，与到达顺序无关。
//  4. 资源有界：仅保存去重集合与每个状态键的最新事件，无无界缓冲。
package pipeline

import (
	"fmt"
	"sync"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/identity"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/source"
)

// Message 是一条待处理输入。
type Message struct {
	Source       model.SourceType
	SourceSystem string // 投递级来源系统；记录内自带时以记录为准
	Body         string // 文本消息体或 JSON 字符串

	// PresetRejection 非空时跳过解析，直接以该拒绝原因进入批次
	// （用于切分阶段就词法不合法的 JSON 片段，保证它仍能让整批回滚）。
	PresetRejection *model.Rejection
}

// Store 是已提交状态：按 (患者, 事件类型, 编码) 保留最新事件，并记录已见事件 ID。
type Store struct {
	mu     sync.Mutex
	seen   map[string]model.NormalizedEvent // 去重键 (来源系统,事件号) -> 已提交事件
	latest map[string]model.NormalizedEvent
}

// NewStore 创建空存储。
func NewStore() *Store {
	return &Store{
		seen:   make(map[string]model.NormalizedEvent),
		latest: make(map[string]model.NormalizedEvent),
	}
}

// dedupKey 是来源感知的事件去重键。
func dedupKey(ev model.NormalizedEvent) string {
	return ev.SourceSystem + "\x00" + ev.EventID
}

// stateKey 是乱序取舍的状态键（含来源系统，不同系统不互相覆盖结论）。
func stateKey(ev model.NormalizedEvent) string {
	return ev.SourceSystem + "|" + ev.CanonicalPID + "|" + ev.EventType + "|" + ev.Code
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

// SeenCount 返回已去重的事件数（用于资源占用观测）。
func (s *Store) SeenCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.seen)
}

// Pipeline 处理批次并提交到 Store。
type Pipeline struct {
	reg *identity.Registry
}

// New 创建管线。
func New(reg *identity.Registry) *Pipeline {
	return &Pipeline{reg: reg}
}

// Result 是批次处理结果。
type Result struct {
	Outcomes   []model.Outcome // 与输入一一对应
	Committed  bool            // 是否已提交
	RolledBack bool            // 是否因部分失败而整批回滚
}

// parse 按来源类型分发解析。
func parse(m Message) (model.RawRecord, *model.Rejection) {
	switch m.Source {
	case model.SourceText:
		rec, rej := source.ParseText(m.Body)
		rec.SourceSystem = source.ResolveSystem("", m.SourceSystem)
		return rec, rej
	case model.SourceJSON:
		return source.ParseJSONWithSystem([]byte(m.Body), m.SourceSystem)
	default:
		return model.RawRecord{}, &model.Rejection{
			Reason: model.RejectMalformed, Detail: "未知来源类型: " + string(m.Source),
		}
	}
}

// ProcessBatch 处理一批消息，语义见包注释。
func (p *Pipeline) ProcessBatch(store *Store, msgs []Message) Result {
	res := Result{Outcomes: make([]model.Outcome, len(msgs))}

	store.mu.Lock()
	defer store.mu.Unlock()

	// 身份登记走事务：批次失败则整笔丢弃，注册表不留“登记了一半”的痕迹。
	itx := p.reg.Begin()
	staged := make([]model.NormalizedEvent, 0, len(msgs))
	stagedSeen := map[string]model.NormalizedEvent{}
	failed := false

	reject := func(i int, rej *model.Rejection) {
		res.Outcomes[i] = model.Outcome{Quality: model.Quality{Accepted: false, Rejection: rej}}
		failed = true
	}

	for i, m := range msgs {
		if m.PresetRejection != nil {
			reject(i, m.PresetRejection)
			continue
		}
		rec, rej := parse(m)
		if rej != nil {
			reject(i, rej)
			continue
		}
		canon := identity.Canonicalize(rec.PatientID)
		if canon == "" {
			reject(i, &model.Rejection{
				Reason: model.RejectMissingField, Field: "patientId",
				Detail: "患者标识归一化后为空", RawHint: rec.PatientID,
			})
			continue
		}
		deid, ok := itx.Resolve(canon, rec.PatientID, rec.Attrs)
		if !ok {
			reject(i, &model.Rejection{
				Reason: model.RejectIdentityConflict, Field: "patientId",
				Detail: "规范键 " + canon + " 的身份属性与已登记值冲突", RawHint: rec.PatientID,
			})
			continue
		}
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
		// 来源感知去重 + 同号载荷冲突判定（含已提交与本批暂存）。
		key := dedupKey(ev)
		prior, dup := stagedSeen[key]
		if !dup {
			if committed, has := store.seen[key]; has {
				prior, dup = committed, true
			}
		}
		if dup {
			if !eventsEqual(prior, ev) {
				reject(i, &model.Rejection{
					Reason: model.RejectEventConflict, Field: "eventId",
					Detail: fmt.Sprintf("来源系统 %q 内事件号 %q 已存在但时间/编码/取值不一致，无法判定是否同一事件",
						ev.SourceSystem, ev.EventID),
					RawHint: rec.EventID,
				})
				continue
			}
			// 完全一致：重复投递，幂等空操作，仍计为接收。
			res.Outcomes[i] = model.Outcome{Event: &ev, Quality: model.Quality{Accepted: true}}
			continue
		}
		stagedSeen[key] = ev
		res.Outcomes[i] = model.Outcome{Event: &ev, Quality: model.Quality{Accepted: true}}
		staged = append(staged, ev)
	}

	if failed {
		itx.Rollback()
		res.RolledBack = true
		return res
	}

	if err := itx.Commit(); err != nil {
		res.RolledBack = true
		res.Outcomes = markCommitConflict(res.Outcomes)
		return res
	}
	for _, ev := range staged {
		store.seen[dedupKey(ev)] = ev
		key := stateKey(ev)
		if cur, has := store.latest[key]; !has || less(cur, ev) {
			store.latest[key] = ev
		}
	}
	res.Committed = true
	return res
}

// eventsEqual 判断同去重键两条事件的可比对载荷是否一致。
func eventsEqual(a, b model.NormalizedEvent) bool {
	return a.CanonicalPID == b.CanonicalPID &&
		a.EventType == b.EventType &&
		a.Code == b.Code &&
		a.OccurredAt.Equal(b.OccurredAt) &&
		a.Value == b.Value
}

// markCommitConflict 在极小概率的事务提交冲突下把结论统一标为拒绝。
func markCommitConflict(outs []model.Outcome) []model.Outcome {
	for i := range outs {
		if outs[i].Quality.Accepted {
			outs[i] = model.Outcome{Quality: model.Quality{Accepted: false, Rejection: &model.Rejection{
				Reason: model.RejectIdentityConflict, Field: "patientId",
				Detail: "批次提交时与并发登记冲突，请重放本批",
			}}}
		}
	}
	return outs
}

// less 定义乱序取舍规则：按 (OccurredAt, EventID) 字典序。
func less(a, b model.NormalizedEvent) bool {
	if !a.OccurredAt.Equal(b.OccurredAt) {
		return a.OccurredAt.Before(b.OccurredAt)
	}
	return a.EventID < b.EventID
}
