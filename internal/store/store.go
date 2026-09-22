// Package store persists pipeline state with transactional
// commit/rollback semantics.
//
// Durability model (no database, plain files):
//
//   - journal/<batchID>.json is written BEFORE processing (WAL).
//   - processing reads only the last committed snapshot and builds a
//     new candidate state; until Commit nothing visible changes.
//   - Commit writes every mutated file via temp-file + fsync + rename
//     (atomic on POSIX), then removes the journal.
//   - Rollback just deletes the journal; committed files are never
//     partially overwritten.
//   - an exclusive flock(LOCK_EX) on lock serializes processes; an
//     in-process mutex serializes goroutines. On Open, any journal
//     left by a crashed process is treated as rolled back and
//     removed.
//
// Replaying a batch after any failure therefore produces exactly the
// outcome of a one-shot success, provided the same store directory
// and de-identification secret are used.
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
)

const (
	dirEvents    = "events"
	dirRejects   = "rejects"
	dirJournal   = "journal"
	fileState    = "state.json"
	fileTokens   = "token-map.json"
	fileLock     = "lock"
	stateVersion = 1
)

type persistedState struct {
	Version    int                     `json:"version"`
	Dedup      map[string]string       `json:"dedup"`
	Identities map[string]model.Gender `json:"genders"`
	Watermark  time.Time               `json:"watermark"`
	Batches    map[string]string       `json:"batches"`
}

// tokenDoc is the persisted re-identification map (pseudonym -> key).
type tokenDoc struct {
	Entries []tokenEntry `json:"entries"`
}

type tokenEntry struct {
	Pseudonym  string `json:"pseudonym"`
	PatientKey string `json:"patientKey"`
}

// Store is the durable, concurrency-safe pipeline state.
type Store struct {
	dir    string
	lockFd *os.File
	mu     sync.Mutex
	state  persistedState
}

// Open opens (or initializes) a store rooted at dir and takes an
// exclusive process-level lock on it.
func Open(dir string) (*Store, error) {
	for _, d := range []string{dir, filepath.Join(dir, dirEvents),
		filepath.Join(dir, dirRejects), filepath.Join(dir, dirJournal)} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, fmt.Errorf("store: mkdir %s: %w", d, err)
		}
	}
	lockPath := filepath.Join(dir, fileLock)
	fd, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("store: open lock: %w", err)
	}
	if err := syscall.Flock(int(fd.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		fd.Close()
		return nil, fmt.Errorf("store: directory %s is locked by another process: %w", dir, err)
	}

	s := &Store{dir: dir, lockFd: fd,
		state: persistedState{
			Version:    stateVersion,
			Dedup:      map[string]string{},
			Identities: map[string]model.Gender{},
			Batches:    map[string]string{},
		},
	}
	if err := s.loadState(); err != nil {
		fd.Close()
		return nil, err
	}
	if err := s.recoverJournals(); err != nil {
		fd.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the lock and closes the store.
func (s *Store) Close() error {
	if s.lockFd != nil {
		_ = syscall.Flock(int(s.lockFd.Fd()), syscall.LOCK_UN)
		return s.lockFd.Close()
	}
	return nil
}

// Snapshot is the committed state visible to a new batch.
type Snapshot struct {
	Dedup      map[string]string
	Identities map[string]model.Gender
	Watermark  time.Time
	Batches    map[string]string
}

// Snapshot returns a deep copy of the committed state.
func (s *Store) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Snapshot{
		Dedup:      copyMap(s.state.Dedup),
		Identities: copyGenderMap(s.state.Identities),
		Watermark:  s.state.Watermark,
		Batches:    copyMap(s.state.Batches),
	}
}

// Events returns committed canonical events ordered deterministically
// (eventTime, then sourceSystem, then recordId).
func (s *Store) Events() ([]model.CanonicalEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readEventDir()
}

// Rejects returns all committed rejected records ordered by
// (receivedSeq, sourceSystem, recordId).
func (s *Store) Rejects() ([]model.RejectedRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readRejectDir()
}

// Commit atomically persists the batch outcome and merged state.
func (s *Store) Commit(batchID string, result *model.BatchResult, snap Snapshot, nextWatermark time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Outcome files first (idempotent overwrite), state last: a crash
	// between the two leaves a journal and no new state, so recovery
	// rolls back and replay regenerates identical files.
	for _, ev := range result.Accepted {
		if err := s.writeJSONFile(s.eventPath(ev), ev, 0o644); err != nil {
			return err
		}
	}
	for _, rj := range result.Rejected {
		if err := s.writeJSONFile(s.rejectPath(rj), rj, 0o644); err != nil {
			return err
		}
	}

	next := persistedState{
		Version:    stateVersion,
		Dedup:      snap.Dedup,
		Identities: snap.Identities,
		Watermark:  nextWatermark,
		Batches:    snap.Batches,
	}
	if err := s.writeJSONFile(filepath.Join(s.dir, fileState), next, 0o644); err != nil {
		return err
	}
	s.state = next

	jPath := s.journalPath(batchID)
	if err := os.Remove(jPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("store: clear journal: %w", err)
	}
	return nil
}

// Rollback discards a batch journal.
func (s *Store) Rollback(batchID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.Remove(s.journalPath(batchID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("store: rollback: %w", err)
	}
	return nil
}

// journalDoc is the WAL payload.
type journalDoc struct {
	BatchID string            `json:"batchId"`
	Records []model.RawRecord `json:"records"`
	Written time.Time         `json:"written"`
}

// WriteJournal records a batch intent before processing (WAL).
func (s *Store) WriteJournal(batchID string, records []model.RawRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	doc := journalDoc{BatchID: batchID, Records: records, Written: time.Now().UTC()}
	return s.writeJSONFile(s.journalPath(batchID), doc, 0o644)
}

// HasJournal reports whether an uncommitted journal exists.
func (s *Store) HasJournal(batchID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := os.Stat(s.journalPath(batchID))
	return err == nil
}

// JournalBatchID returns the batch id recorded in a journal.
func (s *Store) JournalRecords(batchID string) ([]model.RawRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.journalPath(batchID))
	if err != nil {
		return nil, err
	}
	var doc journalDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	return doc.Records, nil
}

func (s *Store) journalPath(batchID string) string {
	return filepath.Join(s.dir, dirJournal, batchID+".json")
}

// LoadTokenMap reads the persisted re-identification map. A missing
// file yields an empty map (fresh store).
func (s *Store) LoadTokenMap() (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(filepath.Join(s.dir, fileTokens))
	if errors.Is(err, os.ErrNotExist) {
		return map[string]string{}, nil
	}
	if err != nil {
		return nil, err
	}
	var doc tokenDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("store: corrupt token map: %w", err)
	}
	out := make(map[string]string, len(doc.Entries))
	for _, e := range doc.Entries {
		out[e.Pseudonym] = e.PatientKey
	}
	return out, nil
}

// SaveTokenMap persists the re-identification map with 0600
// permissions (access-controlled boundary).
func (s *Store) SaveTokenMap(tokens map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	pseudonyms := make([]string, 0, len(tokens))
	for t := range tokens {
		pseudonyms = append(pseudonyms, t)
	}
	sort.Strings(pseudonyms)
	doc := tokenDoc{Entries: make([]tokenEntry, 0, len(pseudonyms))}
	for _, t := range pseudonyms {
		doc.Entries = append(doc.Entries, tokenEntry{Pseudonym: t, PatientKey: tokens[t]})
	}
	return s.writeJSONFile(filepath.Join(s.dir, fileTokens), doc, 0o600)
}

func (s *Store) eventPath(ev model.CanonicalEvent) string {
	return filepath.Join(s.dir, dirEvents,
		fmt.Sprintf("%s__%s.json", safe(ev.SourceSystem), safe(ev.RecordID)))
}

func (s *Store) rejectPath(rj model.RejectedRecord) string {
	// PayloadHash guarantees distinct malformed records (which may
	// lack recordId/seq) do not overwrite each other, while the same
	// bytes deterministically overwrite the same file on replay.
	return filepath.Join(s.dir, dirRejects,
		fmt.Sprintf("%s__%s__%s.json", safe(rj.SourceSystem), safe(rj.RecordID), shortHex(rj.PayloadHash)))
}

func shortHex(h string) string {
	if len(h) > 16 {
		return h[:16]
	}
	if h == "" {
		return "_"
	}
	return h
}

// safe makes a string usable as a single path component.
func safe(in string) string {
	if in == "" {
		return "_"
	}
	out := make([]rune, 0, len(in))
	for _, r := range in {
		switch r {
		case '/', '\\', 0, ':':
			out = append(out, '_')
		default:
			out = append(out, r)
		}
	}
	return string(out)
}

func (s *Store) writeJSONFile(path string, v any, perm os.FileMode) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, perm); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

func (s *Store) loadState() error {
	data, err := os.ReadFile(filepath.Join(s.dir, fileState))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("store: read state: %w", err)
	}
	var ps persistedState
	if err := json.Unmarshal(data, &ps); err != nil {
		return fmt.Errorf("store: corrupt state: %w", err)
	}
	if ps.Version != stateVersion {
		return fmt.Errorf("store: unsupported state version %d", ps.Version)
	}
	if ps.Dedup == nil {
		ps.Dedup = map[string]string{}
	}
	if ps.Identities == nil {
		ps.Identities = map[string]model.Gender{}
	}
	if ps.Batches == nil {
		ps.Batches = map[string]string{}
	}
	s.state = ps
	return nil
}

// recoverJournals removes journals left by crashed processes: no
// state is committed before journal removal, so the batch is rolled
// back and safe to replay.
func (s *Store) recoverJournals() error {
	entries, err := os.ReadDir(filepath.Join(s.dir, dirJournal))
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if err := os.Remove(filepath.Join(s.dir, dirJournal, e.Name())); err != nil {
			return fmt.Errorf("store: recover journal %s: %w", e.Name(), err)
		}
	}
	return nil
}

func (s *Store) readEventDir() ([]model.CanonicalEvent, error) {
	files, err := filepath.Glob(filepath.Join(s.dir, dirEvents, "*.json"))
	if err != nil {
		return nil, err
	}
	out := make([]model.CanonicalEvent, 0, len(files))
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		var ev model.CanonicalEvent
		if err := json.Unmarshal(data, &ev); err != nil {
			return nil, fmt.Errorf("store: corrupt event file %s: %w", f, err)
		}
		out = append(out, ev)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].EventTime.Equal(out[j].EventTime) {
			return out[i].EventTime.Before(out[j].EventTime)
		}
		if out[i].SourceSystem != out[j].SourceSystem {
			return out[i].SourceSystem < out[j].SourceSystem
		}
		return out[i].RecordID < out[j].RecordID
	})
	return out, nil
}

func (s *Store) readRejectDir() ([]model.RejectedRecord, error) {
	files, err := filepath.Glob(filepath.Join(s.dir, dirRejects, "*.json"))
	if err != nil {
		return nil, err
	}
	out := make([]model.RejectedRecord, 0, len(files))
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		var rj model.RejectedRecord
		if err := json.Unmarshal(data, &rj); err != nil {
			return nil, fmt.Errorf("store: corrupt reject file %s: %w", f, err)
		}
		out = append(out, rj)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ReceivedSeq != out[j].ReceivedSeq {
			return out[i].ReceivedSeq < out[j].ReceivedSeq
		}
		if out[i].SourceSystem != out[j].SourceSystem {
			return out[i].SourceSystem < out[j].SourceSystem
		}
		return out[i].RecordID < out[j].RecordID
	})
	return out, nil
}

func copyMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyGenderMap(in map[string]model.Gender) map[string]model.Gender {
	out := make(map[string]model.Gender, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
