package pipeline

import (
	"fmt"
	"sync"
	"testing"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/state"
)

func dText(sys, id, patient, code, ts, value string) Message {
	return Message{Source: model.SourceText, SourceSystem: sys,
		Body: fmt.Sprintf("EVT|%s|%s|observation|%s|%s|%s", id, patient, code, ts, value)}
}

func countPatients(st *state.Store) int {
	return len(st.Registry().ExportMappings())
}

func TestDurableRestartStable(t *testing.T) {
	dir := t.TempDir()
	run := func() (string, int, int) {
		st, err := state.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		res := Durable(st, []Message{
			dText("HIS-A", "e1", "MRN-1", "HR", "2026-09-01T10:00:00Z", "70"),
			dText("HIS-A", "e2", " mrn 1 ", "HR", "2026-09-01T11:00:00Z", "80"),
		})
		if !res.Committed {
			t.Fatalf("batch not committed: %+v", res)
		}
		deid, _, ok := st.Registry().ReverseLookupByCanon("MRN1")
		if !ok {
			t.Fatal("patient not registered")
		}
		return deid, st.SeenCount(), len(st.Latest())
	}
	deid1, seen1, keys1 := run()
	deid2, seen2, keys2 := run() // 同数据在“新进程”里重放
	if deid1 != deid2 || seen1 != seen2 || keys1 != keys2 {
		t.Fatalf("restart/replay drift: (%s,%d,%d) vs (%s,%d,%d)",
			deid1, seen1, keys1, deid2, seen2, keys2)
	}
	if seen1 != 2 || keys1 != 1 {
		t.Fatalf("want seen=2 keys=1, got seen=%d keys=%d", seen1, keys1)
	}
	t.Logf("跨进程重跑：编号=%s 稳定，seen=%d，结论键=%d（重放幂等）", deid1, seen1, keys1)
}

func TestDurableConcurrentAndDuplicateNoExtraIdentity(t *testing.T) {
	dir := t.TempDir()
	st, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				m := dText(fmt.Sprintf("sys%d", g%2), fmt.Sprintf("g%d-e%d", g, i),
					"MRN-777", "HR", fmt.Sprintf("2026-09-01T%02d:%02d:00Z", g, i), "70")
				r := Durable(st, []Message{m})
				if !r.Committed && r.Err == nil {
					t.Errorf("batch g%d-e%d rolled back", g, i)
				}
			}
		}(g)
	}
	wg.Wait()
	if n := countPatients(st); n != 1 {
		t.Fatalf("并发导入同一患者产生 %d 个身份，要求恰好 1", n)
	}
	t.Logf("8 goroutine × 25 批并发后患者身份数=1，去重事件=%d", st.SeenCount())

	// 显式重复导入同一患者同一事件：不增加身份与去重数。
	before := st.SeenCount()
	r := Durable(st, []Message{
		dText("sys0", "g0-e0", "mrn 777", "HR", "2026-09-01T00:00:00Z", "70")})
	if !r.Committed {
		t.Fatal("duplicate should commit")
	}
	if st.SeenCount() != before || countPatients(st) != 1 {
		t.Fatalf("重复导入改变状态 seen=%d patients=%d", st.SeenCount(), countPatients(st))
	}
}

func TestDurableReplayAfterFailureEqualsOneShot(t *testing.T) {
	good1 := dText("HIS-A", "e1", "MRN-1", "HR", "2026-09-01T10:00:00Z", "70")
	bad := dText("HIS-A", "e1", "MRN-1", "HR", "2026-09-01T10:00:00Z", "99") // 同号不同值
	good2 := dText("HIS-A", "e3", "MRN-1", "HR", "2026-09-01T10:02:00Z", "75")

	// 路径 A：先坏批失败，修复后重放（跨“重启”——重新 Open 同一目录）。
	dirA := t.TempDir()
	stA, _ := state.Open(dirA)
	res := Durable(stA, []Message{good1, bad, good2})
	if !res.RolledBack || res.Committed {
		t.Fatalf("坏批应回滚: %+v", res)
	}
	rj := res.Outcomes[1].Quality.Rejection
	if rj.Reason != model.RejectEventConflict {
		t.Fatalf("reason=%s want EVENT_CONFLICT", rj.Reason)
	}
	t.Logf("坏批拒绝原因可区分=%s detail=%s", rj.Reason, rj.Detail)
	if stA.SeenCount() != 0 {
		t.Fatalf("回滚批次不得留痕 seen=%d", stA.SeenCount())
	}
	stA.Close()

	fixed := dText("HIS-A", "e2", "MRN-1", "TEMP", "2026-09-01T10:01:00Z", "36.5")
	stA2, _ := state.Open(dirA)
	rA := Durable(stA2, []Message{good1, fixed, good2})
	if !rA.Committed {
		t.Fatalf("修复后重放应提交: err=%v", rA.Err)
	}
	stA2.Close()

	// 路径 B：一开始就全部成功。
	dirB := t.TempDir()
	stB, _ := state.Open(dirB)
	rB := Durable(stB, []Message{good1, fixed, good2})
	if !rB.Committed {
		t.Fatal("one-shot should commit")
	}

	la, lb := stA2, stB
	defer la.Close()
	defer func() { _ = lb.Close() }()
	if la.SeenCount() != lb.SeenCount() || len(la.Latest()) != len(lb.Latest()) {
		t.Fatalf("规模不一致 replay seen=%d/%d vs one-shot seen=%d/%d",
			la.SeenCount(), len(la.Latest()), lb.SeenCount(), len(lb.Latest()))
	}
	for k, ev := range lb.Latest() {
		if la.Latest()[k] != ev {
			t.Fatalf("结论不一致 key=%s: %+v vs %+v", k, la.Latest()[k], ev)
		}
	}
	idA, _, _ := la.Registry().ReverseLookupByCanon("MRN1")
	idB, _, _ := lb.Registry().ReverseLookupByCanon("MRN1")
	if idA != idB {
		t.Fatalf("编号不一致 %s vs %s", idA, idB)
	}
	t.Logf("失败后修复重放（经重启）与一次性成功完全一致：编号=%s seen=%d 结论键=%d",
		idA, la.SeenCount(), len(la.Latest()))
}

func TestDurableRejectedBatchLeavesNoIdentityOnDisk(t *testing.T) {
	dir := t.TempDir()
	st, _ := state.Open(dir)
	// 新患者 + 非法编码同批：整批回滚，磁盘上不得出现该患者身份。
	msgs := []Message{
		dText("S", "e1", "MRN-NEW", "BOGUS", "2026-09-01T10:00:00Z", "1"),
		dText("S", "e2", "MRN-NEW", "HR", "2026-09-01T10:01:00Z", "70"),
	}
	if r := Durable(st, msgs); !r.RolledBack {
		t.Fatal("expected rollback")
	}
	st.Close()

	st2, _ := state.Open(dir)
	defer st2.Close()
	if n := len(st2.Registry().ExportMappings()); n != 0 {
		t.Fatalf("回滚批次的身份映射落盘了: %d entries", n)
	}
	if st2.SeenCount() != 0 {
		t.Fatalf("回滚批次的事件落盘了 seen=%d", st2.SeenCount())
	}
	t.Log("被拒绝批次重启后无身份、无事件痕迹")
}
