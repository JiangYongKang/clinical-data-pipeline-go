// Package model defines the canonical clinical event model and the
// data-quality verdicts produced by the pipeline.
package model

import "time"

// EventType is the normalized event code.
type EventType string

// Supported canonical event types.
const (
	EventAdmission   EventType = "ADMISSION"
	EventDischarge   EventType = "DISCHARGE"
	EventObservation EventType = "OBSERVATION"
	EventDiagnosis   EventType = "DIAGNOSIS"
)

// Gender is the normalized administrative gender code.
type Gender string

// Supported canonical gender codes.
const (
	GenderMale    Gender = "M"
	GenderFemale  Gender = "F"
	GenderOther   Gender = "O"
	GenderUnknown Gender = "U"
)

// RawRecord is one inbound record before normalization.
type RawRecord struct {
	// SourceSystem identifies the upstream system (e.g. "hospitalA").
	SourceSystem string
	// Kind is "text" (structured text message) or "json" (resource JSON).
	Kind string
	// Payload is the raw message body.
	Payload []byte
	// ReceivedSeq is the pipeline-assigned, monotonically increasing
	// ingestion sequence used for deterministic tie-breaking.
	ReceivedSeq int64
	// ReceivedAt is the ingestion timestamp (wall clock).
	ReceivedAt time.Time
}

// CanonicalEvent is the normalized, identity-resolved record.
type CanonicalEvent struct {
	// RecordID is the source-assigned record identifier.
	RecordID string
	// SourceSystem identifies the upstream system.
	SourceSystem string
	// PatientKey is the stable merged patient key (pre de-identification).
	PatientKey string
	// Pseudonym is the reproducible de-identified patient token.
	Pseudonym string
	// EventType is the normalized event code.
	EventType EventType
	// EventTime is the normalized business event time.
	EventTime time.Time
	// Gender is the normalized administrative gender (may be Unknown).
	Gender Gender
	// ContentHash is the deterministic hash of the normalized payload.
	ContentHash string
}

// Quality is the final data-quality verdict for a record.
type Quality string

// Quality verdicts.
const (
	QualityAccepted Quality = "ACCEPTED"
	QualityRejected Quality = "REJECTED"
)

// RejectReason is the distinguishable reason a record was rejected.
type RejectReason string

// Reject reasons. Every malformed/invalid record maps to exactly one.
const (
	// ReasonMissingRequired: a required field is absent or blank.
	ReasonMissingRequired RejectReason = "MISSING_REQUIRED_FIELD"
	// ReasonInvalidCode: a coded field carries an unsupported value.
	ReasonInvalidCode RejectReason = "INVALID_CODE"
	// ReasonMalformed: the message cannot be parsed at the syntax level.
	ReasonMalformed RejectReason = "MALFORMED_MESSAGE"
	// ReasonStructureMismatch: parsed payload does not match its
	// declared structure (wrong types / wrong resource type).
	ReasonStructureMismatch RejectReason = "STRUCTURE_MISMATCH"
	// ReasonIdentityConflict: the same identity asserts incompatible
	// attributes across records.
	ReasonIdentityConflict RejectReason = "IDENTITY_CONFLICT"
	// ReasonDuplicateConflict: the same (system, record id) carries
	// different content on redelivery.
	ReasonDuplicateConflict RejectReason = "DUPLICATE_CONTENT_CONFLICT"
	// ReasonLateOutOfOrder: event arrived after the order grace window
	// closed and cannot be admitted deterministically.
	ReasonLateOutOfOrder RejectReason = "LATE_OUT_OF_ORDER"
)

// RejectedRecord describes a rejection with a distinguishable reason.
type RejectedRecord struct {
	RecordID     string
	SourceSystem string
	Reason       RejectReason
	// Detail explains the specific field / value driving the decision.
	Detail      string
	Quality     Quality
	PayloadHash string
	ReceivedSeq int64
}

// BatchResult is the deterministic outcome of processing one batch.
type BatchResult struct {
	Accepted []CanonicalEvent
	Rejected []RejectedRecord
	// Duplicates lists dedup keys (system|recordID) redelivered with
	// identical content. They are not applied twice and are not
	// rejected: redelivery is the idempotent no-op case.
	Duplicates []string
}
