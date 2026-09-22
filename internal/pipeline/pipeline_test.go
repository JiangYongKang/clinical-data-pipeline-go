package pipeline

import (
	"fmt"
	"sync"
	"testing"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/identity"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
)

func textMsg(id, patient, code, ts, value string) Message {
	return Message{Source: model.SourceText,
		Body: fmt.Sprintf("EVT|%s|%s|observation|%s|%s|%s", id, patient, code, ts, value)}
}

func setup() (*Pipeline, *Store) {
	return New(identity.NewRegistry()), NewStore()
}

func TestBatchRollbackAndReplay(t *testing.T) {
	p, store := setup()
	good1 := textMsg("e1", "MRN-1", "HR", "2026-09-01T10:00:00Z", "70")
	bad := textMsg("e2", "MRN-1", "NOPE", "2026-09-01T10:01:00Z", "1")
	good2 := textMsg("e3", "MRN-1", "HR", "2026-09-01T10:02:00Z", "75")

	res := p.ProcessBatch(store, []Message{good1, bad, good2})
	if !res.RolledBack || res.Committed {
		t.Fatalf("expected rollback, got %+v", res)
	}
	t.Logf("batch rolled back; reject reason=%s field=%s",
		res.Outcomes[1].Quality.Rejection.Reason, res.Outcomes[1].Quality.Rejection.Field)
	if len(store.Latest()) != 0 || store.SeenCount() != 0 {
		t.Fatal("rolled-back batch must not touch store")
	}

	// 修复后重放：结果与一次性成功一致
	fixed := textMsg("e2", "MRN-1", "TEMP", "2026-09-01T10:01:00Z", "36.5")
	res2 := p.ProcessBatch(store, []Message{good1, fixed, good2})
	if !res2.Committed {
		t.Fatal("replayed batch should commit")
	}

	p2, store2 := setup()
	res3 := p2.ProcessBatch(store2, []Message{good1, fixed, good2})
	if !res3.Committed {
		t.Fatal("one-shot batch should commit")
	}
	a, b := store.Latest(), store2.Latest()
	if len(a) != len(b) {
		t.Fatalf("replay state size %d != one-shot %d", len(a), len(b))
	}
	for k, ev := range a {
		if b[k] != ev {
			t.Fatalf("state mismatch at %s: replay=%+v one-shot=%+v", k, ev, b[k])
		}
	}
	t.Logf("replay-after-fix state identical to one-shot success (%d keys)", len(a))
}

func TestDuplicateDeliveryIdempotent(t *testing.T) {
	p, store := setup()
	m := textMsg("e1", "MRN-1", "HR", "2026-09-01T10:00:00Z", "70")
	p.ProcessBatch(store, []Message{m})
	before := store.Latest()
	// 重复投递同一事件（跨批次）
	res := p.ProcessBatch(store, []Message{m, m})
	if !res.Committed {
		t.Fatal("duplicate batch should commit as no-op")
	}
	after := store.Latest()
	if len(after) != len(before) || store.SeenCount() != 1 {
		t.Fatalf("duplicates changed state: seen=%d keys=%d", store.SeenCount(), len(after))
	}
	for k, ev := range before {
		if after[k] != ev {
			t.Fatalf("duplicate delivery mutated state at %s", k)
		}
	}
	t.Logf("event=%s delivered 3 times -> seen=%d, state unchanged", "e1", store.SeenCount())
}

func TestOutOfOrderDeterministic(t *testing.T) {
	mk := func() ([]Message, []Message) {
		old := textMsg("e1", "MRN-1", "HR", "2026-09-01T10:00:00Z", "70")
		newer := textMsg("e2", "MRN-1", "HR", "2026-09-01T11:00:00Z", "80")
		return []Message{old, newer}, []Message{newer, old} // 正序与乱序
	}
	fwd, rev := mk()
	p1, s1 := setup()
	p1.ProcessBatch(s1, fwd)
	p2, s2 := setup()
	p2.ProcessBatch(s2, rev)

	a, b := s1.Latest(), s2.Latest()
	if len(a) != 1 || len(b) != 1 {
		t.Fatalf("expected 1 state key each, got %d/%d", len(a), len(b))
	}
	for k := range a {
		t.Logf("key=%s in-order winner=%s@%s out-of-order winner=%s@%s",
			k, a[k].EventID, a[k].OccurredAt, b[k].EventID, b[k].OccurredAt)
		if a[k] != b[k] {
			t.Fatalf("out-of-order arrival changed conclusion: %+v vs %+v", a[k], b[k])
		}
		if a[k].EventID != "e2" {
			t.Fatalf("latest OccurredAt should win, got %s", a[k].EventID)
		}
	}
}

func TestConcurrentBatchesSamePatient(t *testing.T) {
	p, store := setup()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				m := textMsg(fmt.Sprintf("g%d-e%d", g, i), " mrn-7 ",
					"HR", fmt.Sprintf("2026-09-01T10:%02d:00Z", i%60), "70")
				if r := p.ProcessBatch(store, []Message{m}); !r.Committed {
					t.Errorf("batch g%d-e%d not committed", g, i)
				}
			}
		}(g)
	}
	wg.Wait()
	latest := store.Latest()
	if len(latest) != 1 {
		t.Fatalf("same patient across goroutines must merge to 1 state key, got %d", len(latest))
	}
	for k, ev := range latest {
		t.Logf("merged key=%s deid=%s winner=%s (seen=%d/400)", k, ev.DeidentifiedID, ev.EventID, store.SeenCount())
	}
	if store.SeenCount() != 400 {
		t.Fatalf("seen=%d, want 400", store.SeenCount())
	}
}

func TestIdentityConflictRollsBack(t *testing.T) {
	reg := identity.NewRegistry()
	p := New(reg)
	store := NewStore()
	jsonWithDOB := func(id, dob string) Message {
		return Message{Source: model.SourceJSON, Body: fmt.Sprintf(
			`{"resourceType":"Observation","id":%q,"subject":"MRN-9","code":"HR",`+
				`"effectiveDateTime":"2026-09-01T10:00:00Z","value":"70","attrs":{"birthDate":%q}}`, id, dob)}
	}
	if r := p.ProcessBatch(store, []Message{jsonWithDOB("e1", "1990-01-01")}); !r.Committed {
		t.Fatal("first batch should commit")
	}
	res := p.ProcessBatch(store, []Message{jsonWithDOB("e2", "1985-05-05")})
	if !res.RolledBack {
		t.Fatal("conflicting identity must roll back")
	}
	rj := res.Outcomes[0].Quality.Rejection
	t.Logf("conflict rejection: reason=%s detail=%s", rj.Reason, rj.Detail)
	if rj.Reason != model.RejectIdentityConflict {
		t.Fatalf("reason=%s, want IDENTITY_CONFLICT", rj.Reason)
	}
	if store.SeenCount() != 1 {
		t.Fatalf("conflict batch must not commit, seen=%d", store.SeenCount())
	}
}

func TestLargeInputBounded(t *testing.T) {
	p, store := setup()
	const n = 20000
	msgs := make([]Message, 0, n)
	for i := 0; i < n; i++ {
		// 同一患者同一编码反复上报：状态键数保持为 1
		msgs = append(msgs, textMsg(fmt.Sprintf("e%d", i), "MRN-1",
			"HR", fmt.Sprintf("2026-09-01T%02d:%02d:%02dZ", i/3600%24, i/60%60, i%60), "70"))
	}
	res := p.ProcessBatch(store, msgs)
	if !res.Committed {
		t.Fatal("large batch should commit")
	}
	if len(store.Latest()) != 1 {
		t.Fatalf("state keys=%d, want 1 (bounded)", len(store.Latest()))
	}
	t.Logf("processed %d events -> state keys=%d, dedup set=%d", n, len(store.Latest()), store.SeenCount())
}
