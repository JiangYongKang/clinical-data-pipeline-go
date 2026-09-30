package pipeline

import (
	"time"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/identity"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/state"
)

// canonicalPID 计算规范患者键（空串表示归一化后为空）。
func canonicalPID(rec model.RawRecord) string {
	return identity.Canonicalize(rec.PatientID)
}

// normalizedFromState 把持久事件转回对外的归一化事件模型。
func normalizedFromState(ev state.Event) *model.NormalizedEvent {
	t, _ := time.Parse(time.RFC3339Nano, ev.OccurredAt)
	return &model.NormalizedEvent{
		EventID:        ev.EventID,
		SourceSystem:   ev.SourceSystem,
		CanonicalPID:   ev.CanonicalPID,
		DeidentifiedID: ev.DeidentifiedID,
		EventType:      ev.EventType,
		Code:           ev.Code,
		OccurredAt:     t,
		Value:          ev.Value,
	}
}
