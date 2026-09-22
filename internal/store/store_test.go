package store

import (
	"testing"
	"time"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
)

func TestCommitReopenReadsBack(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	snap := st.Snapshot()
	ev := model.CanonicalEvent{
		RecordID: "r1", SourceSystem: "s1", PatientKey: "P1",
		Pseudonym: "PSN-x", EventType: model.EventAdmission,
		EventTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Gender:    model.GenderMale, ContentHash: "h1",
	}
	rj := model.RejectedRecord{
		RecordID: "bad", SourceSystem: "s1",
		Reason: model.ReasonInvalidCode, Detail: "d",
		Quality: model.QualityRejected, PayloadHash: "deadbeef",
	}
	snap.Dedup["s1|r1"] = "h1"
	snap.Identities["P1"] = model.GenderMale
	snap.Watermark = ev.EventTime
	if err := st.Commit("b1", &model.BatchResult{
		Accepted: []model.CanonicalEvent{ev},
		Rejected: []model.RejectedRecord{rj},
	}, snap, ev.EventTime); err != nil {
		t.Fatal(err)
	}
	if st.HasJournal("b1") {
		t.Fatal("journal must be removed after commit")
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	events, err := st2.Events()
	if err != nil {
		t.Fatal(err)
	}
	rejects, err := st2.Rejects()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Pseudonym != "PSN-x" {
		t.Fatalf("events=%+v", events)
	}
	if len(rejects) != 1 || rejects[0].Reason != model.ReasonInvalidCode {
		t.Fatalf("rejects=%+v", rejects)
	}
	if !st2.Snapshot().Watermark.Equal(ev.EventTime) {
		t.Fatal("watermark not restored")
	}
	t.Logf("reopened store: events=%d rejects=%d watermark=%s",
		len(events), len(rejects), st2.Snapshot().Watermark.Format(time.RFC3339))
}

func TestExclusiveProcessLock(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := Open(dir); err == nil {
		t.Fatal("second process must not acquire the same store lock")
	} else {
		t.Logf("second Open rejected as required: %v", err)
	}
}

func TestOrphanJournalPurgedOnOpen(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.WriteJournal("ghost", nil); err != nil {
		t.Fatal(err)
	}
	st.Close()

	st2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	if st2.HasJournal("ghost") {
		t.Fatal("orphan journal must be purged as rollback on open")
	}
	t.Log("orphan journal purged on reopen")
}
