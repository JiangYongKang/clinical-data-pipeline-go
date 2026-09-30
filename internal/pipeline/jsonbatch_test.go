package pipeline

import (
	"testing"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
)

// TestMultiObjectFilePerRecordRejectRollsWholeBatch 固化边界：
// JSON 文件按资源对象边界解析（不因缩进/换行/对象数整批报错），
// 但其中任一对象校验失败仍按批次原子性整批回滚，其余好记录也不生效。
func TestMultiObjectFilePerRecordRejectRollsWholeBatch(t *testing.T) {
	p, store := setup()
	msgs := []Message{
		{Source: model.SourceJSON, SourceSystem: "HIS-A", Body: `{"resourceType":"Observation","id":"ok1","subject":"MRN-1","code":"HR","effectiveDateTime":"2026-09-01T00:00:00Z","value":"1"}`},
		{Source: model.SourceJSON, SourceSystem: "HIS-A", Body: `{"resourceType":"Observation","id":"ok2","subject":"MRN-1","code":"HR","effectiveDateTime":"2026-09-01T00:01:00Z","value":"2","extra":1}`},
	}
	r := p.ProcessBatch(store, msgs)
	if !r.RolledBack {
		t.Fatal("one bad object must roll back the whole file batch")
	}
	var got model.RejectReason
	for i, o := range r.Outcomes {
		if o.Quality.Rejection != nil {
			got = o.Quality.Rejection.Reason
			t.Logf("object %d rejected: reason=%s detail=%s",
				i, got, o.Quality.Rejection.Detail)
		}
	}
	if got != model.RejectStructureMismatch {
		t.Fatalf("unknown field must yield STRUCTURE_MISMATCH, got %s", got)
	}
	if store.SeenCount() != 0 {
		t.Fatal("rolled-back multi-object batch must not persist anything")
	}
}
