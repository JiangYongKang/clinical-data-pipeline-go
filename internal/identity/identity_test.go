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
