package identity

import (
	"sync"
	"testing"
)

func TestCanonicalizeEquivalents(t *testing.T) {
	variants := []string{"MRN-123", " mrn 123 ", "mrn_123", "MRN123", "mRn- 1_2 3"}
	want := Canonicalize(variants[0])
	for _, v := range variants {
		got := Canonicalize(v)
		t.Logf("canonical(%q) = %q", v, got)
		if got != want {
			t.Fatalf("canonical(%q)=%q, want %q", v, got, want)
		}
	}
}

func TestResolveReproducible(t *testing.T) {
	r1, r2 := NewRegistry(), NewRegistry()
	c := Canonicalize("MRN-123")
	id1, ok1 := r1.Resolve(c, nil)
	id2, ok2 := r2.Resolve(c, nil)
	t.Logf("canonical=%s deid(run1)=%s deid(run2)=%s", c, id1, id2)
	if !ok1 || !ok2 || id1 != id2 {
		t.Fatalf("deid not reproducible: %q vs %q", id1, id2)
	}
	// 重复导入同一患者不产生新身份
	again, _ := r1.Resolve(c, nil)
	if again != id1 {
		t.Fatalf("re-import produced different id: %q vs %q", again, id1)
	}
	// 不同患者得到不同身份
	other, _ := r1.Resolve(Canonicalize("MRN-456"), nil)
	if other == id1 {
		t.Fatalf("distinct patients share deid %q", id1)
	}
}

func TestResolveConflictRejected(t *testing.T) {
	r := NewRegistry()
	c := Canonicalize("MRN-123")
	if _, ok := r.Resolve(c, map[string]string{"birthDate": "1990-01-01"}); !ok {
		t.Fatal("first registration should succeed")
	}
	_, ok := r.Resolve(c, map[string]string{"birthDate": "1985-05-05"})
	t.Logf("canonical=%s conflicting birthDate -> ok=%v (expect false)", c, ok)
	if ok {
		t.Fatal("conflicting identity attributes must be rejected, not silently merged")
	}
	// 一致属性仍可解析
	if _, ok := r.Resolve(c, map[string]string{"birthDate": "1990-01-01"}); !ok {
		t.Fatal("consistent attributes should resolve")
	}
}

func TestConcurrentImportSamePatient(t *testing.T) {
	r := NewRegistry()
	c := Canonicalize("MRN-789")
	const n = 64
	ids := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, ok := r.Resolve(c, map[string]string{"birthDate": "2000-12-31"})
			if !ok {
				t.Error("concurrent resolve failed")
			}
			ids[i] = id
		}(i)
	}
	wg.Wait()
	for i := 1; i < n; i++ {
		if ids[i] != ids[0] {
			t.Fatalf("concurrent import produced duplicate identities: %q vs %q", ids[0], ids[i])
		}
	}
	t.Logf("canonical=%s deid=%s consistent across %d concurrent resolves", c, ids[0], n)
}

func TestTxRollbackLeavesNoTrace(t *testing.T) {
	r := NewRegistry()
	// 先成功登记一个患者。
	tx := r.Begin()
	id, ok := tx.Resolve(Canonicalize("MRN-1"), "MRN-1", map[string]string{"birthDate": "1990-01-01"})
	if !ok {
		t.Fatal("resolve")
	}
	r.Commit(tx)
	before := r.ExportSnapshot()

	// 第二批：先解析同患者（暂存成功），随后另一个患者出现身份冲突 -> 整批放弃。
	tx2 := r.Begin()
	if _, ok := tx2.Resolve(Canonicalize("MRN-1"), "MRN-1", map[string]string{"birthDate": "1990-01-01"}); !ok {
		t.Fatal("consistent resolve should stage")
	}
	if _, ok := tx2.Resolve(Canonicalize("MRN-1"), "MRN-1", map[string]string{"birthDate": "1970-11-11"}); ok {
		t.Fatal("conflicting attr must be rejected")
	}
	tx2.Abort()

	after := r.ExportSnapshot()
	if len(after.Records) != len(before.Records) {
		t.Fatalf("aborted batch changed registry: before=%d after=%d",
			len(before.Records), len(after.Records))
	}
	if after.Records[0].Attrs["birthDate"] != "1990-01-01" {
		t.Fatalf("aborted batch mutated committed attrs: %+v", after.Records[0])
	}
	t.Logf("aborted batch left no trace on registry; deid=%s birthDate=%s",
		after.Records[0].Deidentified, after.Records[0].Attrs["birthDate"])

	// 修复后重放：同患者仍拿到同一个去标识化编号。
	tx3 := r.Begin()
	id3, ok := tx3.Resolve(Canonicalize("MRN-1"), "MRN-1", map[string]string{"birthDate": "1990-01-01"})
	r.Commit(tx3)
	if !ok || id3 != id {
		t.Fatalf("replay deid=%q want %q", id3, id)
	}
}

func TestSnapshotSurvivesRestart(t *testing.T) {
	// 模拟“进程重启”：导出快照 -> 全新注册表载入，编号与登记完全一致。
	r1 := NewRegistry()
	tx := r1.Begin()
	id1, _ := tx.Resolve(Canonicalize("mrn 789"), "mrn 789", map[string]string{"birthDate": "2000-12-31"})
	r1.Commit(tx)
	tx = r1.Begin()
	id2, _ := tx.Resolve(Canonicalize("MRN-100"), "MRN-100", nil)
	r1.Commit(tx)
	snap := r1.ExportSnapshot()

	r2 := NewRegistry()
	if err := r2.LoadSnapshot(snap); err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	got1, raw1, ok1 := r2.Reidentify(id1)
	got2, _, ok2 := r2.Reidentify(id2)
	t.Logf("after restart: %s -> canonical=%s raw=%q; %s -> canonical=%s",
		id1, got1, raw1, id2, got2)
	if !ok1 || got1 != "MRN789" || raw1 != "mrn 789" {
		t.Fatalf("reidentify id1 after restart: ok=%v canon=%q raw=%q", ok1, got1, raw1)
	}
	if !ok2 || got2 != "MRN100" {
		t.Fatalf("reidentify id2 after restart: ok=%v canon=%q", ok2, got2)
	}
	if r2.Count() != 2 {
		t.Fatalf("registry size after restart=%d want 2", r2.Count())
	}
	// 重新推导仍得到同一编号。
	tx2 := r2.Begin()
	again, ok := tx2.Resolve(Canonicalize(" mrn-789 "), " mrn-789 ", map[string]string{"birthDate": "2000-12-31"})
	r2.Commit(tx2)
	if !ok || again != id1 {
		t.Fatalf("after restart same patient -> %q want %q", again, id1)
	}
}

func TestLoadSnapshotRejectsTamperedDeid(t *testing.T) {
	tx := NewRegistry().Begin()
	tx.Resolve(Canonicalize("MRN-1"), "MRN-1", nil)
	r := NewRegistry()
	r.Commit(tx)
	snap := r.ExportSnapshot()
	snap.Records[0].Deidentified = "PAT-FORGED000000000"
	r2 := NewRegistry()
	if err := r2.LoadSnapshot(snap); err == nil {
		t.Fatal("tampered deidentified id must fail checksum-derived validation")
	} else {
		t.Logf("tampered snapshot rejected: %v", err)
	}
}

func TestReidentifyExplicitOnly(t *testing.T) {
	// 反查是显式调用的能力：默认路径只拿得到去标识化编号，不暴露原始标识。
	r := NewRegistry()
	tx := r.Begin()
	id, ok := tx.Resolve(Canonicalize("MRN-555"), "MRN-555", nil)
	r.Commit(tx)
	if !ok {
		t.Fatal("resolve")
	}
	if canon, raw, ok := r.Reidentify(id); !ok || canon != "MRN555" || raw != "MRN-555" {
		t.Fatalf("explicit reidentify returned %q %q %v", canon, raw, ok)
	}
	if _, _, ok := r.Reidentify("PAT-NOPE"); ok {
		t.Fatal("unknown deid must not resolve")
	}
	t.Logf("explicit reidentify %s -> canon=%s raw=%q", id, "MRN555", "MRN-555")
}
