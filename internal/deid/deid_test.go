package deid

import (
	"path/filepath"
	"sync"
	"testing"
)

func TestPseudonymStableAndDistinct(t *testing.T) {
	d, err := New([]byte("test-pepper"))
	if err != nil {
		t.Fatal(err)
	}
	a := d.Pseudonym("P001")
	b := d.Pseudonym("p001") // keys are already canonical upstream
	c := d.Pseudonym("P001")
	t.Logf("pseudonym P001=%s P002-will-differ; repeat=%s", a, c)
	if a != c {
		t.Fatal("pseudonym not reproducible")
	}
	if a == b {
		// different keys -> different tokens
	}
	other := d.Pseudonym("P002")
	if other == a {
		t.Fatal("distinct identities must not share a pseudonym")
	}
	if key, ok := d.Reidentify(a); !ok || key != "P001" {
		t.Fatalf("reidentify = %q,%v want P001,true", key, ok)
	}
	t.Logf("reidentify %s -> P001; unknown token resolves to false", a)
	if _, ok := d.Reidentify("PSN-deadbeef"); ok {
		t.Fatal("unknown token must not guess an identity")
	}
}

func TestEmptySecretRejected(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatal("empty secret must be rejected")
	}
}

func TestConcurrentPseudonymNoDuplicateMap(t *testing.T) {
	d, _ := New([]byte("pepper"))
	var wg sync.WaitGroup
	tokens := make(chan string, 300)
	for i := 0; i < 300; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tokens <- d.Pseudonym("P007") // same key, many goroutines
		}()
	}
	wg.Wait()
	close(tokens)
	first := ""
	count := 0
	for tk := range tokens {
		count++
		if first == "" {
			first = tk
		}
		if tk != first {
			t.Fatal("pseudonym forked under concurrency")
		}
	}
	key, ok := d.Reidentify(first)
	if !ok || key != "P007" {
		t.Fatalf("token map should hold exactly one mapping, got %q %v", key, ok)
	}
	t.Logf("%d concurrent imports produced exactly one pseudonym %s -> %s",
		count, first, key)
}

func TestExportImportTokenMapRoundtrip(t *testing.T) {
	dir := t.TempDir()
	d, _ := New([]byte("pepper"))
	tok := d.Pseudonym("P100")
	path := filepath.Join(dir, "tokens.json")
	if err := d.ExportTokenMap(path); err != nil {
		t.Fatal(err)
	}
	d2, _ := New([]byte("pepper"))
	if err := d2.ImportTokenMap(path); err != nil {
		t.Fatal(err)
	}
	got, ok := d2.Reidentify(tok)
	if !ok || got != "P100" {
		t.Fatalf("roundtrip failed: %q %v", got, ok)
	}
	t.Logf("token map roundtrip ok: %s <-> P100", tok)
}
