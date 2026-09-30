package state

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/identity"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/pipeline"
)

func mkSnap(t *testing.T) (identity.Snapshot, pipeline.StoreSnapshot) {
	t.Helper()
	reg := identity.NewRegistry()
	tx := reg.Begin()
	deid, ok := tx.Resolve(identity.Canonicalize("MRN-123"), "MRN-123",
		map[string]string{"birthDate": "1990-01-01"})
	if !ok {
		t.Fatal("resolve failed")
	}
	reg.Commit(tx)
	ev := model.NormalizedEvent{
		EventID: "e1", SourceSystem: "HIS-A", CanonicalPID: "MRN123",
		DeidentifiedID: deid, EventType: "observation", Code: "HR",
		OccurredAt: parseTime(t, "2026-09-01T10:00:00Z"), Value: "70",
	}
	return reg.ExportSnapshot(), pipeline.StoreSnapshot{
		Seen:   []model.NormalizedEvent{ev},
		Latest: []model.NormalizedEvent{ev},
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	m := New(filepath.Join(dir, "state"))
	id1, st1 := mkSnap(t)
	if err := m.Save(id1, st1); err != nil {
		t.Fatalf("save: %v", err)
	}
	id2, st2, err := m.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(id2.Records) != 1 || id2.Records[0].Deidentified != id1.Records[0].Deidentified {
		t.Fatalf("identity roundtrip mismatch: %+v", id2)
	}
	if len(st2.Seen) != 1 || st2.Seen[0].EventID != "e1" || st2.Seen[0].SourceSystem != "HIS-A" {
		t.Fatalf("store roundtrip mismatch: %+v", st2)
	}
	// 再存一次更大的快照：目录内不应残留临时文件。
	st1.Latest = append(st1.Latest, st1.Latest[0])
	if err := m.Save(id1, st1); err != nil {
		t.Fatalf("save2: %v", err)
	}
	entries, _ := os.ReadDir(m.Dir())
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".pipeline-state.tmp-") {
			t.Fatalf("atomic write left temp file %s", e.Name())
		}
	}
	t.Logf("snapshot roundtrip ok; deid=%s dir entries=%v",
		id2.Records[0].Deidentified, names(entries))
}

func TestLoadMissingIsEmpty(t *testing.T) {
	m := New(filepath.Join(t.TempDir(), "fresh"))
	id, st, err := m.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(id.Records) != 0 || len(st.Seen) != 0 {
		t.Fatal("missing state file must load as empty")
	}
}

func TestLoadRejectsCorruptAndTruncated(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	m := New(dir)
	id1, st1 := mkSnap(t)
	if err := m.Save(id1, st1); err != nil {
		t.Fatal(err)
	}
	final := filepath.Join(dir, fileName)
	good, _ := os.ReadFile(final)

	for name, mutate := range map[string]func([]byte) []byte{
		"truncated":     func(b []byte) []byte { return b[:len(b)/2] },
		"garbage":       func(b []byte) []byte { return []byte("{not json") },
		"tampered-body": func(b []byte) []byte { return []byte(strings.ReplaceAll(string(b), "HIS-A", "HIS-B")) },
	} {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(final, mutate(good), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, _, err := m.Load(); err == nil {
				t.Fatal("corrupt state must be rejected, not silently read")
			} else {
				t.Logf("%s -> load rejected: %v", name, err)
			}
		})
	}
}

func TestFilePermissionsRestricted(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	m := New(dir)
	if err := m.Save(mkSnap(t)); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o700 {
		t.Fatalf("dir perm=%o, want 0700", perm)
	}
	info, err = os.Stat(filepath.Join(dir, fileName))
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("state file perm=%o, want 0600", perm)
	}
	t.Logf("dir=0%o file=0%o (group/other have no access)",
		info.Mode().Perm()|0o700, info.Mode().Perm())
}

func TestLockExcludesSecondProcess(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	m1 := New(dir)
	if err := m1.Lock(); err != nil {
		t.Fatal(err)
	}
	m2 := New(dir)
	if err := m2.Lock(); err != ErrLocked {
		t.Fatalf("second lock err=%v, want ErrLocked", err)
	}
	t.Log("second concurrent process correctly denied exclusive lock")
	if err := m1.Unlock(); err != nil {
		t.Fatal(err)
	}
	if err := m2.Lock(); err != nil {
		t.Fatalf("lock after unlock should succeed: %v", err)
	}
	_ = m2.Unlock()
}

func TestExportMappingRestricted(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	m := New(filepath.Join(dir, "state"))
	if err := m.Save(mkSnap(t)); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "mapping.json")
	n, err := m.ExportMapping(out)
	if err != nil || n != 1 {
		t.Fatalf("export n=%d err=%v", n, err)
	}
	info, _ := os.Stat(out)
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("mapping export perm=%o, want 0600", perm)
	}
	body, _ := os.ReadFile(out)
	if !strings.Contains(string(body), "rawPatientId") || !strings.Contains(string(body), "MRN-123") {
		t.Fatalf("mapping export missing re-id columns: %s", body)
	}
	t.Logf("mapping export (0600, explicit path): %s", strings.TrimSpace(string(body)))
}

func names(es []os.DirEntry) []string {
	out := make([]string, 0, len(es))
	for _, e := range es {
		out = append(out, e.Name())
	}
	return out
}

func TestLockCleansStaleTempFiles(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	m := New(dir)
	if err := m.Save(mkSnap(t)); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(dir, ".pipeline-state.tmp-crashed")
	if err := os.WriteFile(stale, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := m.Lock(); err != nil {
		t.Fatal(err)
	}
	defer m.Unlock()
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale temp should be removed after lock, stat err=%v", err)
	}
	// 正式状态文件完好。
	id, st, err := m.Load()
	if err != nil || len(id.Records) != 1 || len(st.Seen) != 1 {
		t.Fatalf("state must remain intact after cleanup: %v", err)
	}
	t.Log("stale rename-pre-crash temp removed; committed state intact")
}
