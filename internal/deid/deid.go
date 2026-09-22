// Package deid provides deterministic de-identification and defines
// the boundary of the re-identification map.
//
// Pseudonym rule (reproducible):
//
//	PSN-<hex(HMAC-SHA256(secret, canonicalIdentityKey)[:16])>
//
// The same (secret, key) pair always produces the same token, so
// repeated and concurrent imports never fork an identity. The token is
// one-way without the secret. Re-identification is additionally
// guarded by a server-side token->key map that is exported to a
// separate, access-controlled file and never written next to the
// de-identified event stream.
package deid

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"sort"
	"sync"
)

// Deidentifier derives stable pseudonyms from identity keys.
type Deidentifier struct {
	secret []byte

	mu   sync.Mutex
	keys map[string]string // pseudonym -> canonical key (re-id map)
}

// New builds a Deidentifier from a secret pepper. An empty secret is
// rejected because it would make pseudonyms forgeable.
func New(secret []byte) (*Deidentifier, error) {
	if len(secret) == 0 {
		return nil, os.ErrInvalid
	}
	cp := make([]byte, len(secret))
	copy(cp, secret)
	return &Deidentifier{secret: cp, keys: map[string]string{}}, nil
}

// Pseudonym returns the reproducible token for an identity key. The
// first call registers the token->key mapping used for controlled
// re-identification; repeat calls for the same key return the same
// token, and two different keys can never collide on one token.
func (d *Deidentifier) Pseudonym(key string) string {
	mac := hmac.New(sha256.New, d.secret)
	mac.Write([]byte(key))
	sum := mac.Sum(nil)
	token := "PSN-" + hex.EncodeToString(sum[:16])

	d.mu.Lock()
	defer d.mu.Unlock()
	if existing, ok := d.keys[token]; ok && existing != key {
		// Cryptographically unreachable in practice; fail loud rather
		// than merge two identities behind one token.
		panic("deid: pseudonym collision between " + existing + " and " + key)
	}
	if _, seen := d.keys[token]; !seen {
		d.keys[token] = key
	}
	return token
}

// Reidentify maps a token back to the canonical key. The boolean is
// false (never a guess) when the token is unknown.
func (d *Deidentifier) Reidentify(token string) (string, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	k, ok := d.keys[token]
	return k, ok
}

// ExportTokenMap writes the re-identification map to path with 0600
// permissions (access-controlled boundary). Output is sorted by token
// so the file is byte-stable across runs.
func (d *Deidentifier) ExportTokenMap(path string) error {
	d.mu.Lock()
	snapshot := make(map[string]string, len(d.keys))
	for t, k := range d.keys {
		snapshot[t] = k
	}
	d.mu.Unlock()

	tokens := make([]string, 0, len(snapshot))
	for t := range snapshot {
		tokens = append(tokens, t)
	}
	sort.Strings(tokens)

	type entry struct {
		Pseudonym  string `json:"pseudonym"`
		PatientKey string `json:"patientKey"`
	}
	doc := struct {
		Entries []entry `json:"entries"`
	}{Entries: make([]entry, 0, len(tokens))}
	for _, t := range tokens {
		doc.Entries = append(doc.Entries, entry{Pseudonym: t, PatientKey: snapshot[t]})
	}
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

// ImportTokenMap restores the re-identification map (e.g. on restart).
func (d *Deidentifier) ImportTokenMap(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	doc := struct {
		Entries []struct {
			Pseudonym  string `json:"pseudonym"`
			PatientKey string `json:"patientKey"`
		} `json:"entries"`
	}{}
	if err := json.Unmarshal(data, &doc); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, e := range doc.Entries {
		d.keys[e.Pseudonym] = e.PatientKey
	}
	return nil
}

// LoadMap restores the map from an in-memory snapshot (pseudonym->key).
func (d *Deidentifier) LoadMap(tokens map[string]string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for t, k := range tokens {
		d.keys[t] = k
	}
}

// TokenMap returns a copy of the re-identification map.
func (d *Deidentifier) TokenMap() map[string]string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[string]string, len(d.keys))
	for t, k := range d.keys {
		out[t] = k
	}
	return out
}
