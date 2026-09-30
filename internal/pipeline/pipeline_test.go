package pipeline

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/identity"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
)

func textMsg(id, patient, code, ts, value string) Message {
	return Message{Source: model.SourceText, SourceSystem: "local",
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
		return Message{Source: model.SourceJSON, SourceSystem: "local", Body: fmt.Sprintf(
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

func textMsgSys(system, id, patient, code, ts, value string) Message {
	return Message{Source: model.SourceText, SourceSystem: system,
		Body: fmt.Sprintf("EVT|%s|%s|observation|%s|%s|%s", id, patient, code, ts, value)}
}

func TestCrossSourceSameEventIDNotMerged(t *testing.T) {
	p, store := setup()
	mA := textMsgSys("HIS-A", "EVT-1", "MRN-1", "HR", "2026-09-01T10:00:00Z", "70")
	mB := textMsgSys("HIS-B", "EVT-1", "MRN-1", "HR", "2026-09-01T11:00:00Z", "80")
	if r := p.ProcessBatch(store, []Message{mA, mB}); !r.Committed {
		t.Fatalf("cross-source same id batch must commit: %+v", r)
	}
	if store.SeenCount() != 2 {
		t.Fatalf("same event id in different sources are 2 events, seen=%d want 2", store.SeenCount())
	}
	latest := store.Latest()
	if len(latest) != 1 {
		t.Fatalf("same patient/type/code collapses to 1 state key, got %d", len(latest))
	}
	for _, ev := range latest {
		// 乱序取舍仍按时间取最新：11:00 的 HIS-B 事件胜出，但去重集合是 2。
		t.Logf("winner event=%s system=%s at=%s value=%s (seen=2)",
			ev.EventID, ev.SourceSystem, ev.OccurredAt, ev.Value)
		if ev.SourceSystem != "HIS-B" || ev.Value != "80" {
			t.Fatalf("wrong winner %+v", ev)
		}
	}
}

func TestSameSourceSameIDConflictRejected(t *testing.T) {
	p, store := setup()
	first := textMsgSys("HIS-A", "EVT-9", "MRN-1", "HR", "2026-09-01T10:00:00Z", "70")
	if r := p.ProcessBatch(store, []Message{first}); !r.Committed {
		t.Fatal("first commit")
	}
	// 同来源同号，但取值对不上：明确拒绝，不能静默挑一个，且整批回滚。
	conflict := textMsgSys("HIS-A", "EVT-9", "MRN-1", "HR", "2026-09-01T10:00:00Z", "99")
	r := p.ProcessBatch(store, []Message{conflict})
	if !r.RolledBack {
		t.Fatal("conflicting same-source event must roll back")
	}
	rj := r.Outcomes[0].Quality.Rejection
	t.Logf("same-id conflict -> reason=%s detail=%s", rj.Reason, rj.Detail)
	if rj.Reason != model.RejectEventConflict {
		t.Fatalf("reason=%s want EVENT_CONFLICT", rj.Reason)
	}
	// 状态保持为第一条。
	if ev := store.Latest()[stateKeyOf(t, store, "MRN1|observation|HR")]; ev.Value != "70" {
		t.Fatalf("store mutated by conflict batch: value=%s", ev.Value)
	}
	if store.SeenCount() != 1 {
		t.Fatalf("conflict batch must not add seen, got %d", store.SeenCount())
	}
	// 同来源同号且内容完全一致 -> 幂等重复，不冲突也不新增。
	dup := textMsgSys("HIS-A", "EVT-9", "MRN-1", "HR", "2026-09-01T10:00:00Z", "70")
	r2 := p.ProcessBatch(store, []Message{dup})
	if !r2.Committed || !r2.Outcomes[0].Duplicate {
		t.Fatalf("identical redelivery must be idempotent duplicate: %+v", r2)
	}
	if store.SeenCount() != 1 {
		t.Fatalf("duplicate changed seen count: %d", store.SeenCount())
	}
	t.Log("identical redelivery marked duplicate; store unchanged")
}

func stateKeyOf(t *testing.T, store *Store, key string) string {
	t.Helper()
	for k := range store.Latest() {
		if k == key {
			return k
		}
	}
	t.Fatalf("state key %q not found", key)
	return ""
}

// fakePersister 记录调用并可注入故障。
type fakePersister struct {
	calls   int32
	failNow bool
	lastID  identity.Snapshot
	lastSt  StoreSnapshot
}

func (f *fakePersister) Persist(idSnap identity.Snapshot, storeSnap StoreSnapshot) error {
	atomic.AddInt32(&f.calls, 1)
	f.lastID, f.lastSt = idSnap, storeSnap
	if f.failNow {
		return errors.New("disk full (simulated)")
	}
	return nil
}

func TestPersistFailureRollsBack(t *testing.T) {
	reg := identity.NewRegistry()
	fp := &fakePersister{}
	p := New(reg, WithPersistence(fp))
	store := NewStore()
	first := textMsgSys("HIS-A", "e1", "MRN-1", "HR", "2026-09-01T10:00:00Z", "70")
	if r := p.ProcessBatch(store, []Message{first}); !r.Committed {
		t.Fatal("first batch should commit")
	}

	// 第二个批次落盘失败：内存与身份登记都必须回滚到上一批。
	fp.failNow = true
	second := textMsgSys("HIS-A", "e2", "MRN-2", "TEMP", "2026-09-01T10:05:00Z", "36.5")
	r := p.ProcessBatch(store, []Message{second})
	if r.Err == nil {
		t.Fatal("expected persistence error")
	}
	if r.Committed || r.RolledBack {
		t.Fatalf("failed batch must not be committed/rolled-back flag mix: %+v", r)
	}
	if store.SeenCount() != 1 || reg.Count() != 1 {
		t.Fatalf("state after persist failure: seen=%d identities=%d want 1/1",
			store.SeenCount(), reg.Count())
	}
	t.Logf("persist failure -> %v; memory rolled back (seen=1 identities=1)", r.Err)

	// 磁盘恢复后重放同一批：成功且与从未失败过一致。
	fp.failNow = false
	r2 := p.ProcessBatch(store, []Message{second})
	if !r2.Committed {
		t.Fatalf("replay after disk recovery must commit: %+v", r2)
	}
	if store.SeenCount() != 2 || reg.Count() != 2 {
		t.Fatalf("after replay seen=%d identities=%d want 2/2", store.SeenCount(), reg.Count())
	}
}

func TestConcurrentBatchesWithPersistence(t *testing.T) {
	reg := identity.NewRegistry()
	fp := &fakePersister{}
	p := New(reg, WithPersistence(fp))
	store := NewStore()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				m := textMsgSys("HIS-A", fmt.Sprintf("g%d-e%d", g, i), " mrn-7 ",
					"HR", fmt.Sprintf("2026-09-01T10:%02d:00Z", i%60), "70")
				if r := p.ProcessBatch(store, []Message{m}); !r.Committed {
					t.Errorf("batch g%d-e%d not committed: err=%v", g, i, r.Err)
				}
			}
		}(g)
	}
	wg.Wait()
	if reg.Count() != 1 {
		t.Fatalf("concurrent import created %d identities, want 1", reg.Count())
	}
	if store.SeenCount() != 400 {
		t.Fatalf("seen=%d want 400", store.SeenCount())
	}
	if atomic.LoadInt32(&fp.calls) != 400 {
		t.Fatalf("persist calls=%d want 400 (one atomic snapshot per committed batch)", fp.calls)
	}
	t.Logf("concurrent persistent batches: 1 identity, 400 seen, 400 atomic snapshots")
}

func TestSameSourceSameIDConflictWithinBatch(t *testing.T) {
	p, store := setup()
	// 同一批内：同来源同号，但取值对不上 -> 整批拒绝 EVENT_CONFLICT，不得标 DUP。
	a := textMsgSys("HIS-A", "EVT-x", "MRN-1", "HR", "2026-09-01T10:00:00Z", "70")
	b := textMsgSys("HIS-A", "EVT-x", "MRN-1", "HR", "2026-09-01T10:00:00Z", "71")
	r := p.ProcessBatch(store, []Message{a, b})
	if !r.RolledBack {
		t.Fatal("intra-batch same-id conflict must roll back")
	}
	found := false
	for i, o := range r.Outcomes {
		if !o.Quality.Accepted && o.Quality.Rejection.Reason == model.RejectEventConflict {
			found = true
			t.Logf("outcome %d EVENT_CONFLICT: %s", i, o.Quality.Rejection.Detail)
		}
	}
	if !found {
		t.Fatalf("expected an EVENT_CONFLICT outcome, got %+v", r.Outcomes)
	}
	if store.SeenCount() != 0 {
		t.Fatalf("rolled-back intra-batch conflict must not commit, seen=%d", store.SeenCount())
	}

	// 同一批内：同来源同号且完全一致 -> 批内幂等重复，只产生一个去重键。
	r2 := p.ProcessBatch(store, []Message{a, a})
	if !r2.Committed {
		t.Fatalf("identical intra-batch redelivery must commit: %+v", r2)
	}
	if store.SeenCount() != 1 {
		t.Fatalf("identical intra-batch dup seen=%d want 1", store.SeenCount())
	}
	if !r2.Outcomes[1].Duplicate {
		t.Fatal("second identical record in batch must be marked duplicate")
	}
	t.Log("intra-batch identical redelivery -> single dedup key, second marked DUP")
}
