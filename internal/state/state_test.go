package state

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func testEvent(sys, id, canon, code, at, val string) Event {
	return Event{
		EventID: id, SourceSystem: sys, CanonicalPID: canon,
		DeidentifiedID: "PAT-" + canon, EventType: "observation", Code: code,
		OccurredAt: at, Value: val,
	}
}

func commitBatch(t *testing.T, st *Store, evs ...Event) {
	t.Helper()
	tx := st.Begin()
	for _, ev := range evs {
		deid, ok := tx.ResolveIdentity(ev.CanonicalPID, "RAW-"+ev.CanonicalPID, nil)
		if !ok {
			tx.Rollback()
			t.Fatalf("resolve %s failed", ev.CanonicalPID)
		}
		ev.DeidentifiedID = deid
		if prior, dup := tx.SeenEvent(ev.SourceSystem, ev.EventID); dup && !PayloadEqual(prior, ev) {
			tx.Rollback()
			t.Fatalf("unexpected conflict for %s/%s", ev.SourceSystem, ev.EventID)
		} else if !dup {
			tx.Stage(ev)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func TestRestartPreservesOutcomesAndIDs(t *testing.T) {
	dir := t.TempDir()
	e1 := testEvent("HIS-A", "e1", "MRN1", "HR", "2026-09-01T10:00:00Z", "70")
	e2 := testEvent("HIS-A", "e2", "MRN1", "HR", "2026-09-01T11:00:00Z", "80")

	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	commitBatch(t, st, e1, e2)
	before := st.Latest()
	beforeID, _, _ := st.Registry().ReverseLookupByCanon("MRN1")
	st.Close()

	// 模拟“隔几天换个进程”：重新打开同一目录。
	st2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	after := st2.Latest()
	if len(after) != len(before) {
		t.Fatalf("restart changed key count: %d vs %d", len(after), len(before))
	}
	for k, v := range before {
		if after[k] != v {
			t.Fatalf("结论在重启后不一致 key=%s before=%+v after=%+v", k, v, after[k])
		}
	}
	afterID, _, _ := st2.Registry().ReverseLookupByCanon("MRN1")
	if afterID != beforeID {
		t.Fatalf("去标识化编号在重启后漂移: %s vs %s", beforeID, afterID)
	}
	if st2.SeenCount() != 2 {
		t.Fatalf("seen=%d want 2 after restart", st2.SeenCount())
	}
	t.Logf("跨进程重启：结论=%d 键、编号 %s 保持稳定；重放已提交事件幂等", len(after), afterID)

	// 重放同一批：不产生重复登记，结论不变。
	commitBatch(t, st2, e1, e2)
	if st2.SeenCount() != 2 {
		t.Fatalf("replay changed seen=%d", st2.SeenCount())
	}
}

func TestRollbackLeavesNoTraceAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	st, _ := Open(dir)
	defer st.Close()
	good := testEvent("HIS-A", "e1", "MRN1", "HR", "2026-09-01T10:00:00Z", "70")
	commitBatch(t, st, good)

	// 开一个事务，暂存身份与事件后丢弃：磁盘与内存都不应有痕迹。
	tx := st.Begin()
	deid, ok := tx.ResolveIdentity("MRN9", "RAW-MRN9", map[string]string{"birthDate": "1990-01-01"})
	if !ok {
		t.Fatal("resolve failed")
	}
	bad := testEvent("HIS-A", "e9", "MRN9", "HR", "2026-09-01T10:00:00Z", "70")
	bad.DeidentifiedID = deid
	tx.Stage(bad)
	tx.Rollback()

	if st.SeenCount() != 1 {
		t.Fatalf("rolled-back tx left seen=%d", st.SeenCount())
	}
	st.Close()
	st2, _ := Open(dir)
	defer st2.Close()
	if st2.SeenCount() != 1 {
		t.Fatalf("rolled-back identity/event persisted to disk, seen=%d", st2.SeenCount())
	}
	if canon, _, ok := st2.Registry().ReverseLookup(deid); ok {
		t.Fatalf("回滚批次的身份登记落盘了: deid=%s -> %s", deid, canon)
	}
	t.Logf("整批回滚后重启：seen=%d，未发现 MRN9 的身份痕迹", st2.SeenCount())
}

func TestReplayAfterFixEqualsOneShot(t *testing.T) {
	// 场景：失败批次（事件冲突）修复后重放，与一开始就成功完全一致。
	dir1, dir2 := t.TempDir(), t.TempDir()
	first := testEvent("HIS-A", "e1", "MRN1", "HR", "2026-09-01T10:00:00Z", "70")
	conflict := testEvent("HIS-A", "e1", "MRN1", "HR", "2026-09-01T10:00:00Z", "99")
	fixed := testEvent("HIS-A", "e2", "MRN1", "TEMP", "2026-09-01T10:01:00Z", "36.5")

	replay, _ := Open(dir1)
	commitBatch(t, replay, first)
	// 冲突批次应整批失败：用底层 Tx 复现冲突检测。
	tx := replay.Begin()
	if prior, dup := tx.SeenEvent(conflict.SourceSystem, conflict.EventID); !dup || PayloadEqual(prior, conflict) {
		t.Fatal("expected payload conflict")
	} else {
		t.Logf("坏记录命中 EVENT_CONFLICT: 已存 %s vs 来件 %s", prior.Value, conflict.Value)
	}
	tx.Rollback()
	commitBatch(t, replay, fixed)
	replay.Close()

	oneShot, _ := Open(dir2)
	commitBatch(t, oneShot, first, fixed)
	oneShot.Close()

	a, _ := Open(dir1)
	b, _ := Open(dir2)
	defer a.Close()
	defer b.Close()
	la, lb := a.Latest(), b.Latest()
	if len(la) != len(lb) {
		t.Fatalf("state size mismatch %d vs %d", len(la), len(lb))
	}
	for k, v := range la {
		if lb[k] != v {
			t.Fatalf("修复后重放 != 一次性成功 at %s: %+v vs %+v", k, v, lb[k])
		}
	}
	idA, _, _ := a.Registry().ReverseLookupByCanon("MRN1")
	idB, _, _ := b.Registry().ReverseLookupByCanon("MRN1")
	if idA != idB {
		t.Fatalf("编号不一致 replay=%s oneshot=%s", idA, idB)
	}
	t.Logf("修复后重放与一次性成功完全一致：%d 键、编号 %s", len(la), idA)
}

func TestAtomicWriteNoPartialFile(t *testing.T) {
	dir := t.TempDir()
	st, _ := Open(dir)
	commitBatch(t, st, testEvent("S", "e1", "MRN1", "HR", "2026-09-01T10:00:00Z", "70"))
	st.Close()

	// 状态文件必须是合法 JSON；不得残留临时文件。
	if _, err := os.Stat(filepath.Join(dir, tmpFileName)); !os.IsNotExist(err) {
		t.Fatalf("残留临时文件: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, stateFileName))
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(data) {
		t.Fatal("状态文件不是合法 JSON（疑似半截写入）")
	}
}

func TestCorruptStateRejected(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, stateFileName), []byte(`{"version":1,"seen":{`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("损坏状态文件必须报错，不能当空状态启动")
	} else {
		t.Logf("损坏文件被拒绝启动: %v", err)
	}
}

func TestFilePermissions(t *testing.T) {
	dir := t.TempDir()
	st, _ := Open(dir)
	commitBatch(t, st, testEvent("S", "e1", "MRN1", "HR", "2026-09-01T10:00:00Z", "70"))

	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o700 {
		t.Fatalf("状态目录权限=%o, want 0700", perm)
	}
	info, err = os.Stat(filepath.Join(dir, stateFileName))
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("状态文件权限=%o, want 0600", perm)
	}

	exp := filepath.Join(t.TempDir(), "mapping.json")
	n, err := st.ExportMapping(exp)
	if err != nil {
		t.Fatal(err)
	}
	info, _ = os.Stat(exp)
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("导出文件权限=%o, want 0600", perm)
	}
	t.Logf("目录0700/状态0600/导出0600，导出条目=%d（仅本地核对）", n)
	st.Close()
}

func TestProcessLockExclusive(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := Open(dir); err != ErrLocked {
		t.Fatalf("并发打开同一目录应 ErrLocked, got %v", err)
	}
	t.Log("同机第二进程打开同一状态目录被排他锁拒绝")
}

func TestReverseLookupAndExport(t *testing.T) {
	dir := t.TempDir()
	st, _ := Open(dir)
	defer st.Close()
	commitBatch(t, st, testEvent("S", "e1", "MRN1", "HR", "2026-09-01T10:00:00Z", "70"))
	deid, raw, ok := st.Registry().ReverseLookupByCanon("MRN1")
	if !ok || raw != "RAW-MRN1" {
		t.Fatalf("反查失败 deid=%s raw=%s ok=%v", deid, raw, ok)
	}
	canon, raw2, ok := st.Registry().ReverseLookup(deid)
	if !ok || canon != "MRN1" || raw2 != "RAW-MRN1" {
		t.Fatalf("按 deid 反查失败: %s %s %v", canon, raw2, ok)
	}
	t.Logf("显式反查（本地排查）: deid=%s <-> canonical=%s raw=%q", deid, canon, raw2)
}

func TestInterruptedWriteKeepsOldState(t *testing.T) {
	dir := t.TempDir()
	st, _ := Open(dir)
	commitBatch(t, st, testEvent("S", "e1", "MRN1", "HR", "2026-09-01T10:00:00Z", "70"))

	// 模拟“写到一半被中断”：留下一个半截临时文件，状态文件保持旧版。
	st.Close()
	junk := []byte(`{"version":1,"seen":{"S\x00e2":{` +
		`"eventId":"e2","sourceSystem":"S","canonicalPid":"MRN1"` +
		`,"deidentifiedId":"PAT-MRN1","eventType":"observation","code":"HR"` +
		`,"occurredAt":"2026-09-01T12:00:00Z","value":"81"`) // 故意截断
	if err := os.WriteFile(filepath.Join(dir, tmpFileName), junk, 0o600); err != nil {
		t.Fatal(err)
	}

	// 重新打开：临时文件必须被清掉，读到的仍是上一版完整状态。
	st2, err := Open(dir)
	if err != nil {
		t.Fatalf("中断后应能用旧状态启动: %v", err)
	}
	defer st2.Close()
	if st2.SeenCount() != 1 {
		t.Fatalf("旧状态丢失或被半截数据污染: seen=%d", st2.SeenCount())
	}
	if _, err := os.Stat(filepath.Join(dir, tmpFileName)); !os.IsNotExist(err) {
		t.Fatal("中断残留临时文件未被清理")
	}
	t.Logf("写盘中断后重启：清理临时文件，seen=%d 为旧版完整状态，无半截数据", st2.SeenCount())
}
