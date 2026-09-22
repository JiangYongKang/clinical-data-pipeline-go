// Package pipeline wires parsing, identity resolution, ordering,
// de-identification and transactional commit into batch processing.
//
// # Deterministic decision rules
//
// Duplicate redelivery (same source system + record id):
//   - identical content hash -> idempotent no-op, listed in
//     BatchResult.Duplicates (never applied twice, never rejected);
//   - different content hash  -> rejected as DUPLICATE_CONTENT_CONFLICT.
//
// Out-of-order events:
//   - records are applied ordered by (eventTime, sourceSystem,
//     recordId); ties are therefore independent of arrival order;
//   - the committed watermark is the highest applied eventTime. An
//     event arriving with eventTime < watermark - LateGrace is
//     rejected as LATE_OUT_OF_ORDER (deterministic cut-off, not a
//     silent drop);
//   - events within the grace window are accepted regardless of
//     arrival order (replay reproduces the same accepted set).
//
// Failure/replay:
//   - a batch journal is written before any state change and removed
//     only after atomic commit; internal errors roll the batch back;
//   - replaying the same batch yields the same committed state as a
//     one-shot success.
package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/deid"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/identity"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/normalize"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/store"
)

// Config controls batch processing.
type Config struct {
	// LateGrace bounds how far behind the watermark an event may
	// arrive. Older events are deterministically rejected.
	LateGrace time.Duration
	// ParseWorkers bounds concurrent parser goroutines.
	ParseWorkers int
	// Aliases is the optional legacy patient-id alias table.
	Aliases map[string]string
	// FailHook, when non-nil, is invoked after parsing to simulate
	// mid-batch failure (used by failure/replay tests).
	FailHook func() error
}

// Pipeline processes batches against one store.
type Pipeline struct {
	mu       sync.Mutex // serializes whole batches (state is serial)
	store    *store.Store
	deid     *deid.Deidentifier
	resolver *identity.Resolver
	cfg      Config
}

// New builds a Pipeline, restoring identity state from the store.
func New(st *store.Store, d *deid.Deidentifier, cfg Config) (*Pipeline, error) {
	if cfg.ParseWorkers < 1 {
		cfg.ParseWorkers = 4
	}
	if cfg.LateGrace < 0 {
		return nil, fmt.Errorf("pipeline: negative LateGrace")
	}
	resolver, err := identity.NewResolver(cfg.Aliases)
	if err != nil {
		return nil, err
	}
	resolver.Restore(st.Snapshot().Identities)
	tokens, err := st.LoadTokenMap()
	if err != nil {
		return nil, err
	}
	d.LoadMap(tokens)
	return &Pipeline{store: st, deid: d, resolver: resolver, cfg: cfg}, nil
}

// parseOutcome is the parse result for one raw record.
type parseOutcome struct {
	raw  model.RawRecord
	norm *normalize.Normalized
	rej  *model.RejectedRecord
}

// ProcessBatch applies one batch atomically.
func (p *Pipeline) ProcessBatch(batchID string, records []model.RawRecord) (*model.BatchResult, error) {
	// Whole-batch serialization: the committed state file is updated
	// as one unit, so concurrent batches must not interleave; parsing
	// inside a batch remains concurrent and bounded.
	p.mu.Lock()
	defer p.mu.Unlock()

	if batchID == "" {
		return nil, fmt.Errorf("pipeline: empty batchId")
	}

	// Fast path: a batch already committed under this id is not
	// reprocessed; replay reports the idempotent no-op outcome.
	snap := p.store.Snapshot()
	if _, seen := snap.Batches[batchID]; seen {
		return &model.BatchResult{}, nil
	}

	// WAL: intent recorded before anything observable happens.
	if err := p.store.WriteJournal(batchID, records); err != nil {
		return nil, fmt.Errorf("pipeline: write journal: %w", err)
	}
	fail := func(err error) (*model.BatchResult, error) {
		if rbErr := p.store.Rollback(batchID); rbErr != nil {
			return nil, fmt.Errorf("pipeline: %v; rollback: %v", err, rbErr)
		}
		return nil, err
	}

	// Bounded-concurrency parsing: at most ParseWorkers goroutines,
	// so memory/CPU stay bounded regardless of batch size.
	outcomes := make([]parseOutcome, len(records))
	sem := make(chan struct{}, p.cfg.ParseWorkers)
	var wg sync.WaitGroup
	for i, rec := range records {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, rec model.RawRecord) {
			defer wg.Done()
			defer func() { <-sem }()
			norm, rej := normalize.Parse(rec)
			if rej != nil {
				outcomes[i] = parseOutcome{raw: rec, rej: rej}
				return
			}
			outcomes[i] = parseOutcome{raw: rec, norm: &norm}
		}(i, rec)
	}
	wg.Wait()

	if p.cfg.FailHook != nil {
		if err := p.cfg.FailHook(); err != nil {
			return fail(fmt.Errorf("pipeline: injected mid-batch failure: %w", err))
		}
	}

	result := &model.BatchResult{}

	// Deterministic application order: eventTime, then sourceSystem,
	// then recordId. Parse-level rejections are detached from that
	// order but kept in ingestion order (index).
	type acceptedParse struct {
		idx  int
		norm normalize.Normalized
	}
	var valid []acceptedParse
	for _, oc := range outcomes {
		if oc.rej != nil {
			result.Rejected = append(result.Rejected, *oc.rej)
			continue
		}
		valid = append(valid, acceptedParse{norm: *oc.norm})
	}
	sort.SliceStable(valid, func(i, j int) bool {
		a, b := valid[i].norm, valid[j].norm
		if !a.EventTime.Equal(b.EventTime) {
			return a.EventTime.Before(b.EventTime)
		}
		if a.SourceSystem != b.SourceSystem {
			return a.SourceSystem < b.SourceSystem
		}
		return a.RecordID < b.RecordID
	})

	// Working copies of committed state for this batch.
	dedup := snap.Dedup
	genders := snap.Identities
	watermark := snap.Watermark

	var newWatermark time.Time
	now := time.Now().UTC()

	for _, v := range valid {
		n := v.norm
		dk := dedupKey(n.SourceSystem, n.RecordID)

		// 1) duplicate redelivery against committed or in-batch state.
		if prev, ok := dedup[dk]; ok {
			if prev == n.ContentHash {
				result.Duplicates = append(result.Duplicates, dk)
			} else {
				result.Rejected = append(result.Rejected, model.RejectedRecord{
					RecordID:     n.RecordID,
					SourceSystem: n.SourceSystem,
					Reason:       model.ReasonDuplicateConflict,
					Detail: fmt.Sprintf(
						"redelivery of %s carries different content (stored=%s, incoming=%s)",
						dk, short(prev), short(n.ContentHash)),
					Quality:     model.QualityRejected,
					PayloadHash: n.RawPayloadHash,
				})
			}
			continue
		}

		// 2) identity resolution (also rejects conflicting genders).
		key, rej := p.resolver.Resolve(n.PatientID, n.Gender, n.GenderPresent,
			n.RecordID, n.SourceSystem)
		if rej != nil {
			rej.PayloadHash = n.RawPayloadHash
			result.Rejected = append(result.Rejected, *rej)
			continue
		}

		// 3) out-of-order cut-off.
		if !watermark.IsZero() && n.EventTime.Before(watermark.Add(-p.cfg.LateGrace)) {
			result.Rejected = append(result.Rejected, model.RejectedRecord{
				RecordID:     n.RecordID,
				SourceSystem: n.SourceSystem,
				Reason:       model.ReasonLateOutOfOrder,
				Detail: fmt.Sprintf(
					"eventTime %s is older than watermark %s minus grace %s",
					n.EventTime.Format(time.RFC3339),
					watermark.Format(time.RFC3339), p.cfg.LateGrace),
				Quality:     model.QualityRejected,
				PayloadHash: n.RawPayloadHash,
			})
			continue
		}

		// 4) accept. Merge gender into the working identity map and
		// pseudonymize.
		merged := genders[key]
		if n.GenderPresent && merged == model.GenderUnknown {
			merged = n.Gender
		}
		if n.GenderPresent && merged == "" {
			merged = n.Gender
		}
		genders[key] = merged
		dedup[dk] = n.ContentHash

		ev := model.CanonicalEvent{
			RecordID:     n.RecordID,
			SourceSystem: n.SourceSystem,
			PatientKey:   key,
			Pseudonym:    p.deid.Pseudonym(key),
			EventType:    n.EventType,
			EventTime:    n.EventTime,
			Gender:       merged,
			ContentHash:  n.ContentHash,
		}
		if ev.Gender == "" {
			ev.Gender = model.GenderUnknown
		}
		result.Accepted = append(result.Accepted, ev)

		if newWatermark.IsZero() || ev.EventTime.After(newWatermark) {
			newWatermark = ev.EventTime
		}
	}

	// Parse-level verdicts are deterministic: the same raw bytes
	// always produce the same acceptance/rejection, so no additional
	// bookkeeping is needed for them here.

	if newWatermark.After(watermark) {
		watermark = newWatermark
	}

	// Deterministic ordering of outcomes.
	sort.Slice(result.Accepted, func(i, j int) bool {
		a, b := result.Accepted[i], result.Accepted[j]
		if !a.EventTime.Equal(b.EventTime) {
			return a.EventTime.Before(b.EventTime)
		}
		if a.SourceSystem != b.SourceSystem {
			return a.SourceSystem < b.SourceSystem
		}
		return a.RecordID < b.RecordID
	})
	sort.Strings(result.Duplicates)

	snap.Dedup = dedup
	snap.Identities = genders
	snap.Watermark = watermark
	snap.Batches[batchID] = resultFingerprint(result, now)

	if err := p.store.Commit(batchID, result, snap, watermark); err != nil {
		return fail(fmt.Errorf("pipeline: commit: %w", err))
	}
	// The token map is part of the committed unit: persist it after
	// the state commit; a crash before this line only loses the map
	// file, which is regenerated from committed events on next open
	// (pseudonyms are deterministic given the secret).
	if err := p.store.SaveTokenMap(p.deid.TokenMap()); err != nil {
		return fail(fmt.Errorf("pipeline: persist token map: %w", err))
	}
	return result, nil
}

func dedupKey(system, recordID string) string {
	return system + "|" + recordID
}

func short(hash string) string {
	if len(hash) > 12 {
		return hash[:12]
	}
	return hash
}

// resultFingerprint is the reproducible digest stored per batch,
// making "same batchId => same outcome" externally checkable.
func resultFingerprint(r *model.BatchResult, runTime time.Time) string {
	h := sha256.New()
	add := func(s string) { h.Write([]byte(s)); h.Write([]byte{0}) }
	for _, ev := range r.Accepted {
		add(ev.SourceSystem)
		add(ev.RecordID)
		add(ev.Pseudonym)
		add(string(ev.EventType))
		add(ev.EventTime.Format(time.RFC3339Nano))
		add(string(ev.Gender))
		add(ev.ContentHash)
	}
	for _, rj := range r.Rejected {
		add(rj.SourceSystem)
		add(rj.RecordID)
		add(string(rj.Reason))
		add(rj.Detail)
	}
	for _, d := range r.Duplicates {
		add(d)
	}
	_ = runTime
	return hex.EncodeToString(h.Sum(nil))
}
