package pipeline

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/deid"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/store"
)

func newTestPipeline(t *testing.T, dir string, cfg Config) (*Pipeline, *store.Store, *deid.Deidentifier) {
	t.Helper()
	st, err := store.Open(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	d, err := deid.New([]byte("unit-test-pepper"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ParseWorkers == 0 {
		cfg.ParseWorkers = 4
	}
	if cfg.LateGrace == 0 {
		cfg.LateGrace = 24 * time.Hour
	}
	p, err := New(st, d, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return p, st, d
}

func textRecord(system, body string, seq int64) model.RawRecord {
	return model.RawRecord{
		SourceSystem: system, Kind: "text",
		Payload: []byte(body), ReceivedSeq: seq,
	}
}

func evBody(mid, pid, etype, ts string) string {
	return fmt.Sprintf("MID=%s|PID=%s|TYPE=%s|TIME=%s", mid, pid, etype, ts)
}

func findReject(res *model.BatchResult, id string) *model.RejectedRecord {
	for i := range res.Rejected {
		if res.Rejected[i].RecordID == id {
			return &res.Rejected[i]
		}
	}
	return nil
}

func TestDuplicateAndOutOfOrderDeterministic(t *testing.T) {
	dir := t.TempDir()
	p, st, _ := newTestPipeline(t, dir, Config{})
	defer st.Close()

	// Records delivered out of event-time order.
	records := []model.RawRecord{
		textRecord("A", evBody("e3", "p1", "OBSERVATION", "2026-01-03T09:00:00Z"), 3),
		textRecord("A", evBody("e1", "p1", "ADMISSION", "2026-01-01T09:00:00Z"), 1),
		textRecord("A", evBody("e2", "p1", "DIAGNOSIS", "2026-01-02T09:00:00Z"), 2),
	}
	res, err := p.ProcessBatch("b1", records)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Accepted) != 3 {
		t.Fatalf("accepted=%d want 3; rejects=%+v", len(res.Accepted), res.Rejected)
	}
	if !res.Accepted[0].EventTime.Before(res.Accepted[1].EventTime) {
		t.Fatal("accepted events must be ordered by eventTime regardless of arrival")
	}
	t.Logf("out-of-order delivery applied in canonical order: %s,%s,%s",
		res.Accepted[0].RecordID, res.Accepted[1].RecordID, res.Accepted[2].RecordID)

	// Identical redelivery: idempotent no-op duplicate.
	res2, err := p.ProcessBatch("b2", []model.RawRecord{
		textRecord("A", evBody("e1", " P 1 ", "ADMISSION", "2026-01-01T09:00:00Z"), 4),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.Duplicates) != 1 || res2.Duplicates[0] != "A|e1" {
		t.Fatalf("duplicates=%v want [A|e1]", res2.Duplicates)
	}
	if len(res2.Accepted) != 0 {
		t.Fatal("redelivery must not produce a second event")
	}
	t.Logf("identical redelivery ignored as duplicate key=%s", res2.Duplicates[0])

	// Conflicting redelivery: same id, different content.
	res3, err := p.ProcessBatch("b3", []model.RawRecord{
		textRecord("A", evBody("e1", "p1", "DISCHARGE", "2026-01-05T09:00:00Z"), 5),
	})
	if err != nil {
		t.Fatal(err)
	}
	rj := findReject(res3, "e1")
	if rj == nil || rj.Reason != model.ReasonDuplicateConflict {
		t.Fatalf("expected DUPLICATE_CONTENT_CONFLICT, got %+v", rj)
	}
	t.Logf("conflicting redelivery rejected: record=e1 reason=%s detail=%q",
		rj.Reason, rj.Detail)

	events, err := st.Events()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("committed events=%d want exactly 3", len(events))
	}
}

func TestLateOutOfOrderCutoff(t *testing.T) {
	dir := t.TempDir()
	p, st, _ := newTestPipeline(t, dir, Config{LateGrace: 1 * time.Hour})
	defer st.Close()

	res, err := p.ProcessBatch("b1", []model.RawRecord{
		textRecord("A", evBody("new", "p1", "OBSERVATION", "2026-01-10T10:00:00Z"), 1),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Accepted) != 1 {
		t.Fatalf("accept=%v rejects=%v", res.Accepted, res.Rejected)
	}

	// 3h older than watermark, grace=1h => deterministically rejected.
	res2, err := p.ProcessBatch("b2", []model.RawRecord{
		textRecord("A", evBody("late", "p1", "OBSERVATION", "2026-01-10T06:00:00Z"), 2),
	})
	if err != nil {
		t.Fatal(err)
	}
	rj := findReject(res2, "late")
	if rj == nil || rj.Reason != model.ReasonLateOutOfOrder {
		t.Fatalf("expected LATE_OUT_OF_ORDER, got %+v", rj)
	}
	t.Logf("late event rejected: record=late reason=%s detail=%q", rj.Reason, rj.Detail)

	// Within grace => accepted even though older than watermark.
	res3, err := p.ProcessBatch("b3", []model.RawRecord{
		textRecord("A", evBody("near", "p1", "OBSERVATION", "2026-01-10T09:30:00Z"), 3),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res3.Accepted) != 1 {
		t.Fatalf("near-late event should be accepted, rejects=%v", res3.Rejected)
	}
	t.Logf("within-grace older event accepted: record=near eventTime=2026-01-10T09:30:00Z")
}

func TestFailureRollbackThenReplayEqualsOneShot(t *testing.T) {
	dir := t.TempDir()

	records := []model.RawRecord{
		textRecord("A", evBody("e1", "p1", "ADMISSION", "2026-01-01T09:00:00Z"), 1),
		textRecord("A", evBody("e2", " P1 ", "DIAGNOSIS", "2026-01-02T09:00:00Z"), 2),
		textRecord("A", "MID=e3|PID=p1|TYPE=BOGUS|TIME=2026-01-03T09:00:00Z", 3),
	}

	// Run 1: injected mid-batch failure after parsing => full rollback.
	p1, st1, _ := newTestPipeline(t, dir, Config{FailHook: func() error { return errors.New("disk full") }})
	_, err := p1.ProcessBatch("batchX", records)
	if err == nil {
		t.Fatal("expected injected failure")
	}
	st1.Close()
	t.Logf("mid-batch failure observed, batch rolled back: %v", err)

	// A fresh process opens the same store; orphan journal is purged.
	p2, st2, _ := newTestPipeline(t, dir, Config{})
	if st2.HasJournal("batchX") {
		t.Fatal("journal from crashed process must be purged on open")
	}
	replayed, err := p2.ProcessBatch("batchX", records)
	if err != nil {
		t.Fatal(err)
	}

	// One-shot reference run against an empty store.
	dirRef := t.TempDir()
	pRef, stRef, _ := newTestPipeline(t, dirRef, Config{})
	oneShot, err := pRef.ProcessBatch("batchX", records)
	if err != nil {
		t.Fatal(err)
	}

	if len(replayed.Accepted) != len(oneShot.Accepted) ||
		len(replayed.Rejected) != len(oneShot.Rejected) {
		t.Fatalf("replay/one-shot size mismatch: %+v vs %+v", replayed, oneShot)
	}
	for i := range oneShot.Accepted {
		a, b := replayed.Accepted[i], oneShot.Accepted[i]
		if a != b {
			t.Fatalf("accepted[%d] differs after replay:\n%+v\n%+v", i, a, b)
		}
	}
	for i := range oneShot.Rejected {
		a, b := replayed.Rejected[i], oneShot.Rejected[i]
		if a.RecordID != b.RecordID || a.Reason != b.Reason {
			t.Fatalf("rejected[%d] differs: %+v vs %+v", i, a, b)
		}
	}
	evA, _ := st2.Events()
	evB, _ := stRef.Events()
	if len(evA) != len(evB) || len(evA) != 2 {
		t.Fatalf("committed event sets differ: %d vs %d", len(evA), len(evB))
	}
	for i := range evA {
		if evA[i] != evB[i] {
			t.Fatalf("event %d differs: %+v vs %+v", i, evA[i], evB[i])
		}
	}
	t.Logf("replay == one-shot: accepted=%d rejected=%d (reject reason=%s)",
		len(replayed.Accepted), len(replayed.Rejected), oneShot.Rejected[0].Reason)
	st2.Close()
	stRef.Close()
}

func TestReplaySameBatchIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	p, st, _ := newTestPipeline(t, dir, Config{})
	defer st.Close()

	records := []model.RawRecord{
		textRecord("A", evBody("e1", "p1", "ADMISSION", "2026-01-01T09:00:00Z"), 1),
	}
	r1, err := p.ProcessBatch("dup-batch", records)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := p.ProcessBatch("dup-batch", records)
	if err != nil {
		t.Fatal(err)
	}
	if len(r1.Accepted) != 1 || len(r2.Accepted) != 0 {
		t.Fatal("replaying committed batch id must be an idempotent no-op")
	}
	events, _ := st.Events()
	if len(events) != 1 {
		t.Fatalf("events=%d want 1", len(events))
	}
	t.Logf("same batchId replay idempotent; committed events=1 record=%s pseudonym=%s",
		events[0].RecordID, events[0].Pseudonym)
}

func TestConcurrentBatchesSamePatientNoDuplicateOrFork(t *testing.T) {
	dir := t.TempDir()
	p, st, _ := newTestPipeline(t, dir, Config{ParseWorkers: 2, LateGrace: 365 * 24 * time.Hour})
	defer st.Close()

	const n = 40
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := fmt.Sprintf(
				"MID=ce%03d|PID= patient 5 |TYPE=OBSERVATION|TIME=2026-03-01T%02d:%02d:00Z",
				i, (i/60)%24, i%60)
			_, err := p.ProcessBatch(fmt.Sprintf("cb%03d", i),
				[]model.RawRecord{textRecord("A", body, int64(i))})
			errCh <- err
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	events, err := st.Events()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != n {
		t.Fatalf("events=%d want %d (lost or duplicated under concurrency)", len(events), n)
	}
	pseudo := map[string]bool{}
	for _, ev := range events {
		if ev.PatientKey != "PATIENT5" {
			t.Fatalf("identity fork: %q", ev.PatientKey)
		}
		pseudo[ev.Pseudonym] = true
	}
	if len(pseudo) != 1 {
		t.Fatalf("pseudonyms for one patient = %d, want 1", len(pseudo))
	}
	t.Logf("%d concurrent batches -> %d events, one key PATIENT5, one pseudonym %s",
		n, len(events), events[0].Pseudonym)
}

func TestLargeBatchBoundedWorkers(t *testing.T) {
	dir := t.TempDir()
	// Bounded parser fan-out of 2 over 5k records must complete.
	p, st, _ := newTestPipeline(t, dir, Config{ParseWorkers: 2, LateGrace: 0})
	defer st.Close()

	const n = 5000
	records := make([]model.RawRecord, n)
	for i := range records {
		body := fmt.Sprintf(
			"MID=big%05d|PID=P9|TYPE=OBSERVATION|TIME=2026-01-%02dT%02d:00:00Z",
			i, (i%28)+1, i%24)
		records[i] = textRecord("A", body, int64(i))
	}
	start := time.Now()
	res, err := p.ProcessBatch("big", records)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Accepted) != n {
		t.Fatalf("accepted=%d want %d (rejects=%d)", len(res.Accepted), n, len(res.Rejected))
	}
	events, _ := st.Events()
	if len(events) != n {
		t.Fatalf("committed=%d want %d", len(events), n)
	}
	t.Logf("large batch: %d records with 2 workers in %s; single pseudonym=%s",
		n, time.Since(start).Round(time.Millisecond), events[0].Pseudonym)
}

func TestCrossSystemIdentityMergeAndRepro(t *testing.T) {
	dir := t.TempDir()
	p, st, d := newTestPipeline(t, dir, Config{
		Aliases: map[string]string{"old-90": "P90"},
	})
	defer st.Close()

	res, err := p.ProcessBatch("x", []model.RawRecord{
		textRecord("sysA", evBody("a1", "p90", "ADMISSION", "2026-01-01T00:00:00Z"), 1),
		textRecord("sysB", evBody("b1", " P 9 0 ", "OBSERVATION", "2026-01-01T01:00:00Z"), 2),
		textRecord("sysC", evBody("c1", "old-90", "DIAGNOSIS", "2026-01-01T02:00:00Z"), 3),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Accepted) != 3 {
		t.Fatalf("rejects=%+v", res.Rejected)
	}
	for _, ev := range res.Accepted {
		if ev.PatientKey != "P90" || ev.Pseudonym != res.Accepted[0].Pseudonym {
			t.Fatalf("cross-system merge failed: %+v", ev)
		}
	}
	// Recomputed directly from key+secret must equal stored pseudonym.
	want := d.Pseudonym("P90") // already registered -> same token
	if want != res.Accepted[0].Pseudonym {
		t.Fatalf("pseudonym not reproducible: %s vs %s", want, res.Accepted[0].Pseudonym)
	}
	key, ok := d.Reidentify(want)
	if !ok || key != "P90" {
		t.Fatalf("reidentify failed: %q %v", key, ok)
	}
	t.Logf("three systems merged: P90 via case/space/alias -> pseudonym %s", want)
}
