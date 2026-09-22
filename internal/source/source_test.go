package source

import (
	"testing"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
)

func TestParseTextValid(t *testing.T) {
	rec, rej := ParseText("EVT|e1|MRN-123|observation|HR|2026-09-01T10:00:00Z|72")
	if rej != nil {
		t.Fatalf("unexpected rejection: %+v", rej)
	}
	t.Logf("event=%s patient=%s code=%s -> accepted", rec.EventID, rec.PatientID, rec.Code)
	if rec.EventID != "e1" || rec.PatientID != "MRN-123" || rec.Code != "HR" || rec.Value != "72" {
		t.Fatalf("bad record: %+v", rec)
	}
	if rec.OccurredAt.Hour() != 10 {
		t.Fatalf("bad time: %v", rec.OccurredAt)
	}
}

func TestParseTextRejections(t *testing.T) {
	cases := []struct {
		name   string
		msg    string
		reason model.RejectReason
	}{
		{"empty", "", model.RejectMalformed},
		{"wrong-type", "ADT|e1|p1|observation|HR|2026-09-01T10:00:00Z|72", model.RejectStructureMismatch},
		{"wrong-field-count", "EVT|e1|p1|HR", model.RejectStructureMismatch},
		{"missing-event-id", "EVT||p1|observation|HR|2026-09-01T10:00:00Z|72", model.RejectMissingField},
		{"missing-patient", "EVT|e1||observation|HR|2026-09-01T10:00:00Z|72", model.RejectMissingField},
		{"missing-time", "EVT|e1|p1|observation|HR||72", model.RejectMissingField},
		{"bad-time", "EVT|e1|p1|observation|HR|not-a-time|72", model.RejectStructureMismatch},
		{"bad-code", "EVT|e1|p1|observation|GLUCOSE|2026-09-01T10:00:00Z|5.5", model.RejectInvalidCode},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, rej := ParseText(c.msg)
			if rej == nil {
				t.Fatalf("expected rejection %s, got acceptance", c.reason)
			}
			t.Logf("input=%q -> reason=%s field=%s detail=%s", c.msg, rej.Reason, rej.Field, rej.Detail)
			if rej.Reason != c.reason {
				t.Fatalf("reason=%s, want %s", rej.Reason, c.reason)
			}
		})
	}
}

func TestParseJSONValid(t *testing.T) {
	body := `{"resourceType":"Observation","id":"e9","subject":"mrn 123","code":"TEMP",` +
		`"effectiveDateTime":"2026-09-02T08:30:00Z","value":"36.6","attrs":{"birthDate":"1990-01-01"}}`
	rec, rej := ParseJSON([]byte(body))
	if rej != nil {
		t.Fatalf("unexpected rejection: %+v", rej)
	}
	t.Logf("event=%s patient=%s attrs=%v -> accepted", rec.EventID, rec.PatientID, rec.Attrs)
	if rec.EventID != "e9" || rec.PatientID != "mrn 123" || rec.Attrs["birthDate"] != "1990-01-01" {
		t.Fatalf("bad record: %+v", rec)
	}
}

func TestParseJSONRejections(t *testing.T) {
	cases := []struct {
		name   string
		body   string
		reason model.RejectReason
	}{
		{"not-json", `{oops`, model.RejectStructureMismatch},
		{"wrong-resource-type", `{"resourceType":"Patient","id":"e1","subject":"p","code":"HR","effectiveDateTime":"2026-09-01T00:00:00Z","value":"1"}`, model.RejectStructureMismatch},
		{"unknown-field", `{"resourceType":"Observation","id":"e1","subject":"p","code":"HR","effectiveDateTime":"2026-09-01T00:00:00Z","value":"1","extra":1}`, model.RejectStructureMismatch},
		{"missing-id", `{"resourceType":"Observation","subject":"p","code":"HR","effectiveDateTime":"2026-09-01T00:00:00Z","value":"1"}`, model.RejectMissingField},
		{"missing-subject", `{"resourceType":"Observation","id":"e1","code":"HR","effectiveDateTime":"2026-09-01T00:00:00Z","value":"1"}`, model.RejectMissingField},
		{"bad-code", `{"resourceType":"Observation","id":"e1","subject":"p","code":"XX","effectiveDateTime":"2026-09-01T00:00:00Z","value":"1"}`, model.RejectInvalidCode},
		{"bad-time", `{"resourceType":"Observation","id":"e1","subject":"p","code":"HR","effectiveDateTime":"yesterday","value":"1"}`, model.RejectStructureMismatch},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, rej := ParseJSON([]byte(c.body))
			if rej == nil {
				t.Fatalf("expected rejection %s, got acceptance", c.reason)
			}
			t.Logf("input=%s -> reason=%s field=%s detail=%s", c.body, rej.Reason, rej.Field, rej.Detail)
			if rej.Reason != c.reason {
				t.Fatalf("reason=%s, want %s", rej.Reason, c.reason)
			}
		})
	}
}
