// 持久化批次处理：判定与提交都走 state.Store 的批次事务。
//
// 与内存版 ProcessBatch 共享完全相同的判定规则（解析、身份冲突、来源感知
// 去重、同号事件冲突、乱序取舍），区别仅在于提交时整份状态原子落盘：
// 任一条被拒绝 -> 事务 Rollback，身份映射与去重/结论都不留痕；
// 提交成功 -> 磁盘先于内存更新，中途被杀重启后状态一致，重放幂等。
package pipeline

import (
	"fmt"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/state"
)

// DurableResult 是持久化批次结果。Err 非空表示落盘失败（批次未生效，可整批重放）。
type DurableResult struct {
	Result
	Err error
}

// Durable 在持久状态上原子处理一批消息。
func Durable(st *state.Store, msgs []Message) DurableResult {
	res := DurableResult{Result: Result{Outcomes: make([]model.Outcome, len(msgs))}}
	tx := st.Begin()
	defer func() {
		if !res.Committed && !res.RolledBack {
			tx.Rollback()
		}
	}()

	staged := make([]state.Event, 0, len(msgs))
	stagedSeen := map[string]state.Event{}
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
		canon := canonicalPID(rec)
		if canon == "" {
			reject(i, &model.Rejection{
				Reason: model.RejectMissingField, Field: "patientId",
				Detail: "患者标识归一化后为空", RawHint: rec.PatientID,
			})
			continue
		}
		deid, ok := tx.ResolveIdentity(canon, rec.PatientID, rec.Attrs)
		if !ok {
			reject(i, &model.Rejection{
				Reason: model.RejectIdentityConflict, Field: "patientId",
				Detail: "规范键 " + canon + " 的身份属性与已登记值冲突", RawHint: rec.PatientID,
			})
			continue
		}
		ev := state.Event{
			EventID:        rec.EventID,
			SourceSystem:   rec.SourceSystem,
			CanonicalPID:   canon,
			DeidentifiedID: deid,
			EventType:      rec.EventType,
			Code:           rec.Code,
			OccurredAt:     state.CanonicalTime(rec.OccurredAt.UTC()),
			Value:          rec.Value,
		}
		// 来源感知去重 + 同号载荷冲突（已提交 + 本批暂存统一判定）。
		key := state.DedupKey(ev.SourceSystem, ev.EventID)
		prior, dup := stagedSeen[key]
		if !dup {
			if committed, has := tx.SeenEvent(ev.SourceSystem, ev.EventID); has {
				prior, dup = committed, true
			}
		}
		if dup {
			if !state.PayloadEqual(prior, ev) {
				reject(i, &model.Rejection{
					Reason: model.RejectEventConflict, Field: "eventId",
					Detail: fmt.Sprintf("来源系统 %q 内事件号 %q 已存在但时间/编码/取值不一致，无法判定是否同一事件",
						ev.SourceSystem, ev.EventID),
					RawHint: rec.EventID,
				})
				continue
			}
			res.Outcomes[i] = model.Outcome{
				Event: normalizedFromState(ev), Quality: model.Quality{Accepted: true}}
			continue
		}
		stagedSeen[key] = ev
		res.Outcomes[i] = model.Outcome{
			Event: normalizedFromState(ev), Quality: model.Quality{Accepted: true}}
		staged = append(staged, ev)
	}

	if failed {
		tx.Rollback()
		res.RolledBack = true
		return res
	}
	for _, ev := range staged {
		tx.Stage(ev)
	}
	if err := tx.Commit(); err != nil {
		res.RolledBack = true
		res.Err = err
		return res
	}
	res.Committed = true
	return res
}
