package state

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestMigrateLegacySubsecondState 模拟修复前版本写出的状态：
// 时间串为可变宽度（整秒无小数 / 一位小数），且旧版本按字符串字典序
// 把更早的整秒事件错存为“最新结论”。重新 Open 必须：
//  1. 归一化时间写法（seen 中为固定 9 位小数）；
//  2. 按真实时间重算，结论改为更晚的小数秒事件；
//  3. seen 集合与身份映射不变，重放依旧幂等。
func TestMigrateLegacySubsecondState(t *testing.T) {
	dir := t.TempDir()
	e1 := testEvent("HIS-A", "e1", "MRN1", "HR", "2026-09-01T10:00:00Z", "70")
	e2 := testEvent("HIS-A", "e2", "MRN1", "HR", "2026-09-01T10:00:00.5Z", "71")
	// 手工写一份“旧版本”状态：e1=整秒，e2=.5；旧 latest 错指更早的 e1。
	legacy := onDisk{
		Version: snapshotVers,
		Seen: map[string]Event{
			DedupKey("HIS-A", "e1"): e1,
			DedupKey("HIS-A", "e2"): e2,
		},
		Latest: map[string]Event{
			LatestKey("MRN1", "observation", "HR"): e1,
		},
	}
	buf, err := json.MarshalIndent(legacy, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, stateFileName), buf, 0o600); err != nil {
		t.Fatal(err)
	}

	st, err := Open(dir)
	if err != nil {
		t.Fatalf("旧状态应可迁移打开: %v", err)
	}
	defer st.Close()

	if st.SeenCount() != 2 {
		t.Fatalf("迁移不得改变 seen 集合，实际 seen=%d", st.SeenCount())
	}
	latest := st.Latest()
	win, ok := latest["MRN1|observation|HR"]
	if !ok {
		t.Fatalf("结论键缺失: %v", latest)
	}
	if win.EventID != "e2" {
		t.Fatalf("迁移后应按真实时间留更晚的 e2(.5)，实际留 %s", win.EventID)
	}
	if win.OccurredAt != "2026-09-01T10:00:00.500000000Z" {
		t.Fatalf("结论时间串未归一化: %q", win.OccurredAt)
	}
	if got, ok := st.seen[DedupKey("HIS-A", "e1")]; !ok ||
		got.OccurredAt != "2026-09-01T10:00:00.000000000Z" {
		t.Fatalf("seen 中整秒时间串未归一化: %+v ok=%v", got, ok)
	}
	t.Logf("旧状态迁移：seen=2 不增删，错误结论 e1(整秒) 已修正为 e2(.5)，时间写法已固定宽度归一化")

	// 迁移后的状态落一次盘并再次重启：结论保持修正后结果，无漂移。
	tx := st.Begin()
	if err := tx.Commit(); err != nil {
		t.Fatalf("空批次提交失败: %v", err)
	}
	st.Close()

	st2, err := Open(dir)
	if err != nil {
		t.Fatalf("迁移落盘后重启失败: %v", err)
	}
	defer st2.Close()
	if w := st2.Latest()["MRN1|observation|HR"]; w.EventID != "e2" {
		t.Fatalf("迁移落盘重启后结论漂移: %s", w.EventID)
	}
	if st2.SeenCount() != 2 {
		t.Fatalf("迁移落盘重启后 seen 漂移: %d", st2.SeenCount())
	}
	t.Logf("迁移结果落盘并重启：结论=e2、seen=2 稳定（重放幂等）")
}
