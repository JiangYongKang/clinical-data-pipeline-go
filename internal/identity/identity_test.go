package identity

import (
	"sync"
	"testing"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
)

func TestCanonicalKeyEquivalences(t *testing.T) {
	cases := []string{"p-001", "P001", " P 001 ", "p:001", "P_0/01"}
	keys := map[string]bool{}
	for _, c := range cases {
		k, ok := CanonicalKey(c)
		if !ok {
			t.Fatalf("CanonicalKey(%q) not ok", c)
		}
		t.Logf("canonical: %-10q -> %q", c, k)
		keys[k] = true
	}
	if len(keys) != 1 {
		t.Fatalf("equivalent representations diverged: %v", keys)
	}

	if _, ok := CanonicalKey("  - "); ok {
		t.Fatal("empty-after-normalization id must be rejected")
	}
}

func TestResolverMergeAndConflict(t *testing.T) {
	r, err := NewResolver(map[string]string{"leg-9": "p-001"})
	if err != nil {
		t.Fatal(err)
	}

	key1, rej := r.Resolve(" P 001 ", model.GenderMale, true, "r1", "sysA")
	if rej != nil {
		t.Fatalf("unexpected rejection: %v", rej)
	}
	key2, rej := r.Resolve("p-001", model.GenderUnknown, false, "r2", "sysB")
	if rej != nil {
		t.Fatalf("unknown gender must not conflict: %v", rej)
	}
	key3, rej := r.Resolve("leg-9", model.GenderMale, true, "r3", "sysC")
	if rej != nil {
		t.Fatalf("alias merge rejected: %v", rej)
	}
	t.Logf("merged keys: %q %q %q (alias leg-9 -> p001)", key1, key2, key3)
	if key1 != key2 || key2 != key3 {
		t.Fatalf("expected stable merge, got %s/%s/%s", key1, key2, key3)
	}
	if g := r.Gender(key1); g != model.GenderMale {
		t.Fatalf("gender = %s, want M", g)
	}

	// Known-vs-known conflict must be rejected, not silently chosen.
	_, rej = r.Resolve("p001", model.GenderFemale, true, "r4", "sysA")
	if rej == nil || rej.Reason != model.ReasonIdentityConflict {
		t.Fatalf("expected IDENTITY_CONFLICT, got %v", rej)
	}
	t.Logf("conflict rejected as required: record=r4 reason=%s detail=%q",
		rej.Reason, rej.Detail)
}

func TestResolverAliasCycleRejected(t *testing.T) {
	if _, err := NewResolver(map[string]string{"a": "b", "b": "a"}); err == nil {
		t.Fatal("alias cycle must fail at resolver construction")
	}
}

func TestResolverConcurrentSamePatientNoFork(t *testing.T) {
	r, err := NewResolver(nil)
	if err != nil {
		t.Fatal(err)
	}
	const n = 200
	var wg sync.WaitGroup
	keys := make(chan string, n)
	errs := make(chan *model.RejectedRecord, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Only half assert gender to avoid deterministic conflicts;
			// the point is concurrent resolve must not fork the identity.
			var g model.Gender
			present := i%2 == 0
			if present {
				g = model.GenderFemale
			}
			k, rej := r.Resolve(" patient 77 ", g, present, "rec", "sys")
			if rej != nil {
				errs <- rej
				return
			}
			keys <- k
		}(i)
	}
	wg.Wait()
	close(keys)
	close(errs)
	for rej := range errs {
		t.Fatalf("unexpected conflict under concurrency: %s", rej.Detail)
	}
	first := ""
	for k := range keys {
		if first == "" {
			first = k
		}
		if k != first {
			t.Fatalf("identity forked under concurrency: %s vs %s", first, k)
		}
	}
	t.Logf("concurrent imports converged on single key %q", first)
}
