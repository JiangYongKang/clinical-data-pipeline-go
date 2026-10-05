package pipeline

import "testing"

// 内存版（未持久化）路径同样按真实时间点取舍小数秒事件，
// 与持久版规则保持一致。
func TestInMemorySubsecondSelection(t *testing.T) {
	old := textMsg("e1", "MRN-1", "HR", "2026-09-01T10:00:00Z", "70")
	newer := textMsg("e2", "MRN-1", "HR", "2026-09-01T10:00:00.5Z", "71")

	// 乱序到达（晚的在前）。
	p, store := setup()
	if r := p.ProcessBatch(store, []Message{newer, old}); !r.Committed {
		t.Fatalf("batch not committed: %+v", r)
	}
	latest := store.Latest()
	if len(latest) != 1 {
		t.Fatalf("want 1 key, got %d", len(latest))
	}
	for _, ev := range latest {
		if ev.EventID != "e2" {
			t.Fatalf("内存版也应留真实时间更晚的 e2(.5)，实际留 %s", ev.EventID)
		}
		if ev.OccurredAt.Nanosecond() != 500000000 {
			t.Fatalf("小数秒精度丢失: nanos=%d", ev.OccurredAt.Nanosecond())
		}
		t.Logf("内存版乱序：胜出=%s 纳秒=%d（小数秒精度保留，按真实时间取舍）",
			ev.EventID, ev.OccurredAt.Nanosecond())
	}
}
