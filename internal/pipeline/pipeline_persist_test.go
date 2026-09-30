package pipeline_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/identity"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/pipeline"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/state"
)

func ptextMsgSys(system, id, patient, code, ts, value string) pipeline.Message {
	return pipeline.Message{Source: model.SourceText, SourceSystem: system,
		Body: fmt.Sprintf("EVT|%s|%s|observation|%s|%s|%s", id, patient, code, ts, value)}
}

func bootPersistent(t *testing.T, dir string) (*pipeline.Pipeline, *pipeline.Store, *state.Manager) {
	t.Helper()
	mgr := state.New(dir)
	if err := mgr.Lock(); err != nil {
		t.Fatal(err)
	}
	idSnap, stSnap, err := mgr.Load()
	if err != nil {
		t.Fatal(err)
	}
	reg := identity.NewRegistry()
	if err := reg.LoadSnapshot(idSnap); err != nil {
		t.Fatal(err)
	}
	store := pipeline.NewStore()
	if err := store.LoadSnapshot(stSnap); err != nil {
		t.Fatal(err)
	}
	return pipeline.New(reg, pipeline.WithPersistence(mgr)), store, mgr
}

func TestCrossProcessRestartSameConclusions(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")

	// 第一个“进程”：提交两个患者、乱序事件。
	p, store, mgr := bootPersistent(t, dir)
	batch1 := []pipeline.Message{
		ptextMsgSys("HIS-A", "e1", "MRN-1", "HR", "2026-09-01T10:00:00Z", "70"),
		ptextMsgSys("HIS-A", "e2", "MRN-2", "HR", "2026-09-01T09:00:00Z", "60"),
	}
	if r := p.ProcessBatch(store, batch1); !r.Committed {
		t.Fatalf("batch1: %+v", r)
	}
	batch2 := []pipeline.Message{
		ptextMsgSys("HIS-A", "e3", "MRN-1", "HR", "2026-09-01T12:00:00Z", "82"),
	}
	if r := p.ProcessBatch(store, batch2); !r.Committed {
		t.Fatalf("batch2: %+v", r)
	}
	before := store.Latest()
	if err := mgr.Unlock(); err != nil {
		t.Fatal(err)
	}

	// “重启”：全新注册表/存储/管线，从同一目录载入。
	p2, store2, mgr2 := bootPersistent(t, dir)
	defer mgr2.Unlock()
	after := store2.Latest()
	if len(after) != len(before) {
		t.Fatalf("state keys after restart %d != %d", len(after), len(before))
	}
	for k, ev := range before {
		if after[k] != ev {
			t.Fatalf("conclusion changed across restart at %s:\n before=%+v\n after =%+v",
				k, ev, after[k])
		}
	}
	for k, ev := range after {
		t.Logf("after restart identical: %s -> event=%s deid=%s value=%s",
			k, ev.EventID, ev.DeidentifiedID, ev.Value)
	}

	// 重启后重复导入：每条都是幂等重复，不产生新身份、新去重键。
	r := p2.ProcessBatch(store2, batch1)
	if !r.Committed {
		t.Fatalf("redelivery after restart: %+v", r)
	}
	for i, o := range r.Outcomes {
		if !o.Duplicate {
			t.Fatalf("outcome %d after restart must be duplicate", i)
		}
	}
	if store2.SeenCount() != 3 {
		t.Fatalf("seen after redelivery=%d want 3", store2.SeenCount())
	}
}

func TestReplayAfterFailedBatchEqualsOneShot(t *testing.T) {
	dirA := filepath.Join(t.TempDir(), "state")

	good1 := ptextMsgSys("HIS-A", "e1", "MRN-1", "HR", "2026-09-01T10:00:00Z", "70")
	bad := ptextMsgSys("HIS-A", "e2", "MRN-1", "NOPE", "2026-09-01T10:01:00Z", "1")
	good2 := ptextMsgSys("HIS-A", "e3", "MRN-1", "HR", "2026-09-01T10:02:00Z", "75")
	fixed := ptextMsgSys("HIS-A", "e2", "MRN-1", "TEMP", "2026-09-01T10:01:00Z", "36.5")

	// 路径 A：失败 -> 重启 -> 修好重放。
	pA, sA, mA := bootPersistent(t, dirA)
	if r := pA.ProcessBatch(sA, []pipeline.Message{good1, bad, good2}); !r.RolledBack {
		t.Fatal("bad batch must roll back")
	}
	if sA.SeenCount() != 0 {
		t.Fatal("nothing may be committed from rolled-back batch")
	}
	if err := mA.Unlock(); err != nil {
		t.Fatal(err)
	}

	pA2, sA2, mA2 := bootPersistent(t, dirA)
	if r := pA2.ProcessBatch(sA2, []pipeline.Message{good1, fixed, good2}); !r.Committed {
		t.Fatal("fixed batch must commit")
	}
	conclA := sA2.Latest()
	idSnapA, _, err := mA2.Load()
	if err != nil {
		t.Fatal(err)
	}
	if err := mA2.Unlock(); err != nil {
		t.Fatal(err)
	}

	// 路径 B：一次性成功（全新目录）。
	dirB := filepath.Join(t.TempDir(), "state")
	pB, sB, mB := bootPersistent(t, dirB)
	if r := pB.ProcessBatch(sB, []pipeline.Message{good1, fixed, good2}); !r.Committed {
		t.Fatal("one-shot batch must commit")
	}
	conclB := sB.Latest()
	idSnapB, _, err := mB.Load()
	if err != nil {
		t.Fatal(err)
	}
	if err := mB.Unlock(); err != nil {
		t.Fatal(err)
	}

	if sA2.SeenCount() != sB.SeenCount() || sA2.SeenCount() != 3 {
		t.Fatalf("seen mismatch: replay=%d oneshot=%d", sA2.SeenCount(), sB.SeenCount())
	}
	if len(conclA) != len(conclB) {
		t.Fatalf("state key count mismatch: %d vs %d", len(conclA), len(conclB))
	}
	for k, evA := range conclA {
		if evA != conclB[k] {
			t.Fatalf("conclusion mismatch at %s replay=%+v oneshot=%+v", k, evA, conclB[k])
		}
	}
	if len(idSnapA.Records) != len(idSnapB.Records) {
		t.Fatalf("identity count mismatch %d vs %d", len(idSnapA.Records), len(idSnapB.Records))
	}
	deidA := map[string]string{}
	for _, rec := range idSnapA.Records {
		deidA[rec.Canonical] = rec.Deidentified
	}
	for _, rec := range idSnapB.Records {
		if deidA[rec.Canonical] != rec.Deidentified {
			t.Fatalf("deidentified id mismatch for %s: %s vs %s",
				rec.Canonical, deidA[rec.Canonical], rec.Deidentified)
		}
	}
	t.Logf("replay-after-failure identical to one-shot: %d conclusions, %d identities",
		len(conclA), len(idSnapB.Records))
}
