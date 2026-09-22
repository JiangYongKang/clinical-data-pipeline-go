// Package identity performs cross-source patient identity resolution.
//
// Merge rules (deterministic, no fuzzy matching):
//
//  1. Identifiers are reduced to a canonical key by:
//     Unicode upper-casing, removing every Unicode whitespace rune,
//     and removing the separators '-', '_', ':' and '/'.
//     Thus " p-001 ", "P001" and "P 001" are the same identity.
//  2. An optional legacy alias table (also canonicalized at build
//     time) redirects an id to an established identity; this is the
//     ONLY mechanism that merges two differently-spelled ids.
//  3. Gender assertions attached to one identity must be consistent:
//     a known gender (M/F/O) conflicting with a different known
//     gender is rejected with IDENTITY_CONFLICT. Unknown (U or absent)
//     never conflicts and a later known assertion fills the gap.
//
// Anything empty after canonicalization, or part of an invalid alias
// chain, is rejected rather than merged with a random identity.
package identity

import (
	"fmt"
	"strings"
	"sync"
	"unicode"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
)

// Resolver merges equivalent patient identifiers and rejects
// undecidable / conflicting identities. It is safe for concurrent use.
type Resolver struct {
	mu      sync.Mutex
	aliases map[string]string // canonical id -> canonical target
	genders map[string]model.Gender
}

// NewResolver builds a Resolver with an optional legacy-id alias
// table. Invalid (empty-after-normalization) entries and alias cycles
// are reported rather than silently ignored.
func NewResolver(aliases map[string]string) (*Resolver, error) {
	r := &Resolver{
		aliases: map[string]string{},
		genders: map[string]model.Gender{},
	}
	for rawFrom, rawTo := range aliases {
		from, okFrom := CanonicalKey(rawFrom)
		to, okTo := CanonicalKey(rawTo)
		if !okFrom || !okTo {
			return nil, fmt.Errorf("identity alias empty after normalization: %q -> %q", rawFrom, rawTo)
		}
		r.aliases[from] = to
	}
	// Resolve each alias chain once; detect cycles.
	for from := range r.aliases {
		seen := map[string]bool{from: true}
		cur := from
		for {
			next, ok := r.aliases[cur]
			if !ok {
				break
			}
			if seen[next] {
				return nil, fmt.Errorf("identity alias cycle involving %q", from)
			}
			seen[next] = true
			cur = next
		}
		// Point directly at the chain's terminal identity.
		if terminal := r.follow(from); terminal != from {
			r.aliases[from] = terminal
		}
	}
	return r, nil
}

// follow walks alias links; the caller guarantees acyclicity.
func (r *Resolver) follow(key string) string {
	for {
		next, ok := r.aliases[key]
		if !ok {
			return key
		}
		key = next
	}
}

// CanonicalKey returns the normalized identity key.
//
// Rules: Unicode upper-case, strip every whitespace rune, strip the
// separator characters '-', '_', ':' and '/'. ok is false when the
// identifier is empty after normalization.
func CanonicalKey(rawID string) (key string, ok bool) {
	var b strings.Builder
	for _, r := range strings.ToUpper(strings.TrimSpace(rawID)) {
		if unicode.IsSpace(r) {
			continue
		}
		switch r {
		case '-', '_', ':', '/':
			continue
		}
		b.WriteRune(r)
	}
	out := b.String()
	if out == "" {
		return "", false
	}
	return out, true
}

// Resolve maps a raw patient id to a stable key and records the
// asserted gender. Conflicting assertions for the same key are
// rejected instead of arbitrarily choosing one. The merge is
// deterministic: unknown yields to known, equal known values agree.
func (r *Resolver) Resolve(rawID string, gender model.Gender, genderPresent bool, recordID, sourceSystem string) (string, *model.RejectedRecord) {
	key, ok := CanonicalKey(rawID)
	if !ok {
		return "", &model.RejectedRecord{
			RecordID:     recordID,
			SourceSystem: sourceSystem,
			Reason:       model.ReasonMissingRequired,
			Detail:       "patientId is empty after canonicalization",
			Quality:      model.QualityRejected,
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	key = r.follow(key)
	existing := r.genders[key]
	incoming := model.GenderUnknown
	if genderPresent {
		incoming = gender
	}
	if isKnown(existing) && isKnown(incoming) && existing != incoming {
		return key, &model.RejectedRecord{
			RecordID:     recordID,
			SourceSystem: sourceSystem,
			Reason:       model.ReasonIdentityConflict,
			Detail: fmt.Sprintf(
				"identity %s already asserts gender %s; record asserts %s",
				key, existing, incoming),
			Quality: model.QualityRejected,
		}
	}
	if !isKnown(existing) && isKnown(incoming) {
		r.genders[key] = incoming
	} else if _, seen := r.genders[key]; !seen {
		r.genders[key] = model.GenderUnknown
	}
	return key, nil
}

// Has reports whether the resolver already holds the (canonical) key.
func (r *Resolver) Has(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.genders[r.follow(key)]
	return ok
}

// Gender returns the merged gender for a canonical key.
func (r *Resolver) Gender(key string) model.Gender {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.genders[r.follow(key)]
}

// Snapshot returns the current key -> merged gender map.
func (r *Resolver) Snapshot() map[string]model.Gender {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]model.Gender, len(r.genders))
	for k, v := range r.genders {
		out[k] = v
	}
	return out
}

// Restore populates the resolver from committed state.
func (r *Resolver) Restore(genders map[string]model.Gender) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for k, v := range genders {
		r.genders[k] = v
	}
}

func isKnown(g model.Gender) bool {
	return g == model.GenderMale || g == model.GenderFemale || g == model.GenderOther
}
