// Package normalize parses the two supported source formats into a
// source-neutral intermediate record.
//
// Structured text messages are pipe-delimited key=value lines:
//
//	MID=E001|PID= P-001 |TYPE=ADMISSION|TIME=2026-01-01T10:00:00Z|GENDER=M
//
// Resource JSON is a fixed-shape ClinicalEvent object. Unknown JSON
// fields and wrong field types are rejected rather than ignored.
package normalize

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
)

// Normalized is the parse output before identity resolution.
type Normalized struct {
	RecordID      string
	SourceSystem  string
	PatientID     string
	EventType     model.EventType
	EventTime     time.Time
	Gender        model.Gender
	GenderPresent bool
	ContentHash   string
	// RawPayloadHash is the SHA-256 of the inbound bytes.
	RawPayloadHash string
}

var validEventTypes = map[model.EventType]bool{
	model.EventAdmission:   true,
	model.EventDischarge:   true,
	model.EventObservation: true,
	model.EventDiagnosis:   true,
}

var validGenders = map[model.Gender]bool{
	model.GenderMale:    true,
	model.GenderFemale:  true,
	model.GenderOther:   true,
	model.GenderUnknown: true,
}

// Parse normalizes one raw record.
//
// A non-nil rejection means the record is invalid and must not be
// silently dropped: the returned RejectedRecord carries a
// distinguishable Reason and a field-level Detail.
func Parse(rec model.RawRecord) (Normalized, *model.RejectedRecord) {
	n := Normalized{}

	// Source system is assigned by the ingestion layer; its absence
	// means the record cannot be placed in any provenance namespace.
	if strings.TrimSpace(rec.SourceSystem) == "" {
		return n, reject(rec, model.ReasonMissingRequired,
			"sourceSystem is empty")
	}
	n.SourceSystem = strings.TrimSpace(rec.SourceSystem)

	switch rec.Kind {
	case "text":
		if rj := parseText(rec.Payload, &n); rj != nil {
			return Normalized{}, rj
		}
	case "json":
		if rj := parseJSON(rec.Payload, &n); rj != nil {
			return Normalized{}, rj
		}
	default:
		return n, reject(rec, model.ReasonMalformed,
			fmt.Sprintf("unknown record kind %q", rec.Kind))
	}

	n.RawPayloadHash = sha256Hex(rec.Payload)
	n.ContentHash = contentHash(n)
	return n, nil
}

func sha256Hex(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func reject(rec model.RawRecord, reason model.RejectReason, detail string) *model.RejectedRecord {
	sum := sha256.Sum256(rec.Payload)
	return &model.RejectedRecord{
		SourceSystem: rec.SourceSystem,
		Reason:       reason,
		Detail:       detail,
		Quality:      model.QualityRejected,
		PayloadHash:  hex.EncodeToString(sum[:]),
		ReceivedSeq:  rec.ReceivedSeq,
	}
}

// parseText parses the pipe-delimited key=value message.
func parseText(payload []byte, n *Normalized) *model.RejectedRecord {
	raw := strings.TrimSpace(string(payload))
	if raw == "" {
		return rejectFromBytes(model.ReasonMalformed, payload,
			"empty text message", n.SourceSystem)
	}

	seen := map[string]string{}
	for _, seg := range strings.Split(raw, "|") {
		kv := strings.SplitN(seg, "=", 2)
		if len(kv) != 2 {
			return rejectFromBytes(model.ReasonMalformed, payload,
				fmt.Sprintf("segment %q is not key=value", seg), n.SourceSystem)
		}
		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(kv[1])
		switch key {
		case "MID", "PID", "TYPE", "TIME", "GENDER":
		default:
			return rejectFromBytes(model.ReasonStructureMismatch, payload,
				fmt.Sprintf("unknown field %q", key), n.SourceSystem)
		}
		if _, dup := seen[key]; dup {
			return rejectFromBytes(model.ReasonStructureMismatch, payload,
				fmt.Sprintf("duplicate field %q", key), n.SourceSystem)
		}
		seen[key] = val
	}

	for _, key := range []string{"MID", "PID", "TYPE", "TIME"} {
		if strings.TrimSpace(seen[key]) == "" {
			return rejectFromBytes(model.ReasonMissingRequired, payload,
				fmt.Sprintf("required field %s is absent or blank", key),
				n.SourceSystem)
		}
	}

	n.RecordID = seen["MID"]
	n.PatientID = seen["PID"]

	et := model.EventType(strings.ToUpper(seen["TYPE"]))
	if !validEventTypes[et] {
		return rejectFromBytes(model.ReasonInvalidCode, payload,
			fmt.Sprintf("TYPE=%q is not a supported event code", seen["TYPE"]),
			n.SourceSystem)
	}
	n.EventType = et

	t, err := time.Parse(time.RFC3339, seen["TIME"])
	if err != nil {
		return rejectFromBytes(model.ReasonStructureMismatch, payload,
			fmt.Sprintf("TIME=%q is not RFC3339: %v", seen["TIME"], err),
			n.SourceSystem)
	}
	n.EventTime = t.UTC()

	if gv, ok := seen["GENDER"]; ok {
		g := model.Gender(strings.ToUpper(gv))
		if !validGenders[g] {
			return rejectFromBytes(model.ReasonInvalidCode, payload,
				fmt.Sprintf("GENDER=%q is not one of M/F/O/U", gv),
				n.SourceSystem)
		}
		n.Gender = g
		n.GenderPresent = true
	}
	return nil
}

// clinicalEventJSON is the fixed-shape resource representation.
type clinicalEventJSON struct {
	ResourceType string  `json:"resourceType"`
	RecordID     *string `json:"recordId"`
	PatientID    *string `json:"patientId"`
	EventType    *string `json:"eventType"`
	EventTime    *string `json:"eventTime"`
	Gender       *string `json:"gender"`
}

// parseJSON parses a fixed-shape resource JSON object.
func parseJSON(payload []byte, n *Normalized) *model.RejectedRecord {
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()

	var res clinicalEventJSON
	if err := dec.Decode(&res); err != nil {
		// Unknown fields and wrong value types violate the declared
		// structure; other decoder failures are syntax-level malformed.
		reason := model.ReasonMalformed
		if strings.Contains(err.Error(), "unknown field") ||
			strings.Contains(err.Error(), "cannot unmarshal") {
			reason = model.ReasonStructureMismatch
		}
		return rejectFromBytes(reason, payload,
			fmt.Sprintf("invalid JSON: %v", err), n.SourceSystem)
	}
	// Exactly one root value: trailing tokens are malformed, trailing
	// whitespace is allowed.
	if _, err := dec.Token(); err == nil {
		return rejectFromBytes(model.ReasonMalformed, payload,
			"trailing data after JSON root object", n.SourceSystem)
	} else if !errors.Is(err, io.EOF) {
		return rejectFromBytes(model.ReasonMalformed, payload,
			fmt.Sprintf("invalid JSON trailer: %v", err), n.SourceSystem)
	}

	if res.ResourceType != "ClinicalEvent" {
		return rejectFromBytes(model.ReasonStructureMismatch, payload,
			fmt.Sprintf("resourceType=%q, want ClinicalEvent", res.ResourceType),
			n.SourceSystem)
	}

	for field, v := range map[string]*string{
		"recordId":  res.RecordID,
		"patientId": res.PatientID,
		"eventType": res.EventType,
		"eventTime": res.EventTime,
	} {
		if v == nil || strings.TrimSpace(*v) == "" {
			return rejectFromBytes(model.ReasonMissingRequired, payload,
				fmt.Sprintf("required field %q is absent or blank", field),
				n.SourceSystem)
		}
	}

	n.RecordID = strings.TrimSpace(*res.RecordID)
	n.PatientID = strings.TrimSpace(*res.PatientID)

	et := model.EventType(strings.ToUpper(strings.TrimSpace(*res.EventType)))
	if !validEventTypes[et] {
		return rejectFromBytes(model.ReasonInvalidCode, payload,
			fmt.Sprintf("eventType=%q is not a supported event code", *res.EventType),
			n.SourceSystem)
	}
	n.EventType = et

	t, err := time.Parse(time.RFC3339, strings.TrimSpace(*res.EventTime))
	if err != nil {
		return rejectFromBytes(model.ReasonStructureMismatch, payload,
			fmt.Sprintf("eventTime=%q is not RFC3339: %v", *res.EventTime, err),
			n.SourceSystem)
	}
	n.EventTime = t.UTC()

	if res.Gender != nil {
		g := model.Gender(strings.ToUpper(strings.TrimSpace(*res.Gender)))
		if !validGenders[g] {
			return rejectFromBytes(model.ReasonInvalidCode, payload,
				fmt.Sprintf("gender=%q is not one of M/F/O/U", *res.Gender),
				n.SourceSystem)
		}
		n.Gender = g
		n.GenderPresent = true
	}
	return nil
}

func rejectFromBytes(reason model.RejectReason, payload []byte, detail, system string) *model.RejectedRecord {
	sum := sha256.Sum256(payload)
	return &model.RejectedRecord{
		SourceSystem: system,
		Reason:       reason,
		Detail:       detail,
		Quality:      model.QualityRejected,
		PayloadHash:  hex.EncodeToString(sum[:]),
	}
}

// contentHash is the deterministic fingerprint of the normalized
// payload. It is independent of identity representation (PID case /
// whitespace differences do not change it) but sensitive to every
// business field.
func contentHash(n Normalized) string {
	return hashFields(
		n.SourceSystem,
		n.RecordID,
		string(n.EventType),
		n.EventTime.UTC().Format(time.RFC3339Nano),
		string(n.Gender),
	)
}

func hashFields(fields ...string) string {
	h := sha256.New()
	for _, f := range fields {
		h.Write([]byte(f))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}
