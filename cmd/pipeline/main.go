// Command pipeline runs the clinical data ingestion pipeline over a
// local inbox directory.
//
// Inbox layout (all files processed in deterministic filename order):
//
//	inbox/<sourceSystem>/<anything>.txt   structured text messages
//	inbox/<sourceSystem>/<anything>.json  resource JSON messages
//
// Files prefixed with "BATCH-<id>__" are grouped into that batch;
// remaining files form batch "batch-ad-hoc-<timestamp>".
//
// Usage:
//
//	pipeline -inbox ./inbox -state ./state -secret secret-file \
//	         [-grace 24h] [-workers 4] [-aliases aliases.json]
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/deid"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/pipeline"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/store"
)

func main() {
	var (
		inbox      = flag.String("inbox", "./inbox", "inbox directory")
		stateDir   = flag.String("state", "./state", "state directory")
		secretFile = flag.String("secret", "./secret.key", "file containing the de-id pepper")
		grace      = flag.Duration("grace", 24*time.Hour, "out-of-order grace window")
		workers    = flag.Int("workers", 4, "concurrent parser workers (bounded)")
		aliasPath  = flag.String("aliases", "", "optional patient-id alias table JSON")
	)
	flag.Parse()

	secret, err := os.ReadFile(*secretFile)
	if err != nil {
		log.Fatalf("read secret: %v", err)
	}
	if len(strings.TrimSpace(string(secret))) == 0 {
		log.Fatal("secret is empty")
	}

	var aliases map[string]string
	if *aliasPath != "" {
		data, err := os.ReadFile(*aliasPath)
		if err != nil {
			log.Fatalf("read aliases: %v", err)
		}
		if err := json.Unmarshal(data, &aliases); err != nil {
			log.Fatalf("parse aliases: %v", err)
		}
	}

	batches, err := loadInbox(*inbox)
	if err != nil {
		log.Fatalf("load inbox: %v", err)
	}
	if len(batches) == 0 {
		log.Print("inbox empty; nothing to do")
		return
	}

	st, err := store.Open(*stateDir)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	d, err := deid.New(secret)
	if err != nil {
		log.Fatalf("deidentifier: %v", err)
	}

	p, err := pipeline.New(st, d, pipeline.Config{
		LateGrace:    *grace,
		ParseWorkers: *workers,
		Aliases:      aliases,
	})
	if err != nil {
		log.Fatalf("new pipeline: %v", err)
	}

	totalAcc, totalRej, totalDup := 0, 0, 0
	for _, b := range batches {
		res, err := p.ProcessBatch(b.id, b.records)
		if err != nil {
			log.Fatalf("batch %s failed and was rolled back: %v", b.id, err)
		}
		for _, ev := range res.Accepted {
			log.Printf("[ACCEPT] batch=%s system=%s record=%s patient=%s pseudonym=%s type=%s eventTime=%s",
				b.id, ev.SourceSystem, ev.RecordID, ev.PatientKey,
				ev.Pseudonym, ev.EventType, ev.EventTime.Format(time.RFC3339))
		}
		for _, rj := range res.Rejected {
			log.Printf("[REJECT] batch=%s system=%s record=%s reason=%s detail=%q",
				b.id, rj.SourceSystem, rj.RecordID, rj.Reason, rj.Detail)
		}
		for _, dk := range res.Duplicates {
			log.Printf("[DUP]    batch=%s key=%s identical redelivery ignored", b.id, dk)
		}
		totalAcc += len(res.Accepted)
		totalRej += len(res.Rejected)
		totalDup += len(res.Duplicates)
	}

	// The token map is committed into the state directory with 0600
	// permissions as part of each batch; we only report its path.
	tokenPath := filepath.Join(*stateDir, "token-map.json")
	if _, err := os.Stat(tokenPath); err != nil {
		log.Fatalf("token map missing after processing: %v", err)
	}
	log.Printf("done: accepted=%d rejected=%d duplicates=%d; re-identification map (0600) at %s",
		totalAcc, totalRej, totalDup, tokenPath)
}

type loadedBatch struct {
	id      string
	records []model.RawRecord
}

func loadInbox(root string) ([]loadedBatch, error) {
	groups := map[string][]model.RawRecord{}
	var order []string
	var seq int64

	walkErr := filepath.WalkDir(root, func(path string, de os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if de.IsDir() {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(path))
		var kind string
		switch ext {
		case ".txt":
			kind = "text"
		case ".json":
			kind = "json"
		default:
			return nil
		}
		// Parent directory name is the source system.
		system := filepath.Base(filepath.Dir(path))
		if system == filepath.Base(root) || system == "" || system == "." {
			return fmt.Errorf("%s: file must live under a <sourceSystem> subdirectory", path)
		}
		base := filepath.Base(path)
		batchID := "batch-ad-hoc"
		if rest, ok := strings.CutPrefix(base, "BATCH-"); ok {
			if id, _, ok2 := strings.Cut(rest, "__"); ok2 {
				batchID = "batch-" + id
			}
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if _, ok := groups[batchID]; !ok {
			order = append(order, batchID)
		}
		seq++
		groups[batchID] = append(groups[batchID], model.RawRecord{
			SourceSystem: system,
			Kind:         kind,
			Payload:      data,
			ReceivedSeq:  seq,
			ReceivedAt:   time.Now().UTC(),
		})
		return nil
	})
	if walkErr != nil {
		return nil, walkErr
	}
	sort.Strings(order)
	out := make([]loadedBatch, 0, len(order))
	for _, id := range order {
		out = append(out, loadedBatch{id: id, records: groups[id]})
	}
	return out, nil
}
