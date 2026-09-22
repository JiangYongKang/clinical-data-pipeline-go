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
	"sync"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/identity"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/source"
)

// Message 是一条待处理输入。
type Message struct {
	Source model.SourceType
	Body   string // 文本消息体或 JSON 字符串
}

// Store 是已提交状态：按 (患者, 事件类型, 编码) 保留最新事件，并记录已见事件 ID。
type Store struct {
	mu     sync.Mutex
	seen   map[string]struct{}
	latest map[string]model.NormalizedEvent
}

// NewStore 创建空存储。
func NewStore() *Store {
	return &Store{
		seen:   make(map[string]struct{}),
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
		return source.ParseText(m.Body)
	case model.SourceJSON:
		return source.ParseJSON([]byte(m.Body))
	default:
		return model.RawRecord{}, &model.Rejection{
			Reason: model.RejectMalformed, Detail: "未知来源类型: " + string(m.Source),
		}
	}
}

// ProcessBatch 处理一批消息，语义见包注释。
func (p *Pipeline) ProcessBatch(store *Store, msgs []Message) Result {
	res := Result{Outcomes: make([]model.Outcome, len(msgs))}
	staged := make([]model.NormalizedEvent, 0, len(msgs))
	failed := false

	for i, m := range msgs {
		rec, rej := parse(m)
		if rej == nil {
			canon := identity.Canonicalize(rec.PatientID)
			if canon == "" {
				rej = &model.Rejection{
					Reason: model.RejectMissingField, Field: "patientId",
					Detail: "患者标识归一化后为空", RawHint: rec.PatientID,
				}
			} else if deid, ok := p.reg.Resolve(canon, rec.Attrs); !ok {
				rej = &model.Rejection{
					Reason: model.RejectIdentityConflict, Field: "patientId",
					Detail: "规范键 " + canon + " 的身份属性与已登记值冲突", RawHint: rec.PatientID,
				}
			} else {
				ev := model.NormalizedEvent{
					EventID:        rec.EventID,
					CanonicalPID:   canon,
					DeidentifiedID: deid,
					EventType:      rec.EventType,
					Code:           rec.Code,
					OccurredAt:     rec.OccurredAt,
					Value:          rec.Value,
				}
				res.Outcomes[i] = model.Outcome{Event: &ev, Quality: model.Quality{Accepted: true}}
				staged = append(staged, ev)
				continue
			}
		}
		res.Outcomes[i] = model.Outcome{Quality: model.Quality{Accepted: false, Rejection: rej}}
		failed = true
	}

	if failed {
		// 回滚：不触碰 store，已提交状态保持不变。
		res.RolledBack = true
		return res
	}

	store.mu.Lock()
	for _, ev := range staged {
		if _, dup := store.seen[ev.EventID]; dup {
			continue // 重复投递：幂等空操作
		}
		store.seen[ev.EventID] = struct{}{}
		key := stateKey(ev)
		if cur, has := store.latest[key]; !has || less(cur, ev) {
			store.latest[key] = ev
		}
	}
	store.mu.Unlock()
	res.Committed = true
	return res
}

// less 定义乱序取舍规则：按 (OccurredAt, EventID) 字典序。
func less(a, b model.NormalizedEvent) bool {
	if !a.OccurredAt.Equal(b.OccurredAt) {
		return a.OccurredAt.Before(b.OccurredAt)
	}
	return a.EventID < b.EventID
}
