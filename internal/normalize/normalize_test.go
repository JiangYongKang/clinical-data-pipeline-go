package normalize

import (
	"testing"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
)

func textRec(body string) model.RawRecord {
	return model.RawRecord{SourceSystem: "hospitalA", Kind: "text", Payload: []byte(body)}
}
func jsonRec(body string) model.RawRecord {
	return model.RawRecord{SourceSystem: "hospitalB", Kind: "json", Payload: []byte(body)}
}

func TestParseTextHappyPathAndCaseFolding(t *testing.T) {
	body := "MID=E1|PID= p-001 |TYPE=admission|TIME=2026-01-01T10:00:00Z|GENDER=m"
	n, rej := Parse(textRec(body))
	if rej != nil {
		t.Fatalf("unexpected rejection: %s %s", rej.Reason, rej.Detail)
	}
	t.Logf("parsed: record=%s patient=%q type=%s time=%s gender=%s hash=%s",
		n.RecordID, n.PatientID, n.EventType, n.EventTime, n.Gender, n.ContentHash[:12])
	if n.RecordID != "E1" || n.EventType != model.EventAdmission || n.Gender != model.GenderMale {
		t.Fatal("normalized fields wrong")
	}

	// Same semantic content, different PID whitespace/case => same content hash.
	body2 := "MID=E1|PID=P001|TYPE=ADMISSION|TIME=2026-01-01T10:00:00Z|GENDER=M"
	n2, rej := Parse(textRec(body2))
	if rej != nil {
		t.Fatalf("unexpected rejection: %s", rej.Detail)
	}
	if n2.ContentHash != n.ContentHash {
		t.Fatalf("content hash should ignore PID representation: %s vs %s",
			n.ContentHash[:12], n2.ContentHash[:12])
	}
	t.Logf("content hash stable across PID representations: %s", n.ContentHash[:12])
}

func TestParseRejectionReasons(t *testing.T) {
	cases := []struct {
		name   string
		rec    model.RawRecord
		reason model.RejectReason
	}{
		{"empty text", textRec("   "), model.ReasonMalformed},
		{"not key value", textRec("GARBAGE"), model.ReasonMalformed},
		{"missing MID", textRec("PID=p1|TYPE=ADMISSION|TIME=2026-01-01T10:00:00Z"), model.ReasonMissingRequired},
		{"blank PID", textRec("MID=e|PID=  |TYPE=ADMISSION|TIME=2026-01-01T10:00:00Z"), model.ReasonMissingRequired},
		{"missing TIME", textRec("MID=e|PID=p1|TYPE=ADMISSION"), model.ReasonMissingRequired},
		{"bad event code", textRec("MID=e|PID=p1|TYPE=BOGUS|TIME=2026-01-01T10:00:00Z"), model.ReasonInvalidCode},
		{"bad gender code", textRec("MID=e|PID=p1|TYPE=ADMISSION|TIME=2026-01-01T10:00:00Z|GENDER=X"), model.ReasonInvalidCode},
		{"bad time", textRec("MID=e|PID=p1|TYPE=ADMISSION|TIME=not-a-time"), model.ReasonStructureMismatch},
		{"unknown field", textRec("MID=e|PID=p1|TYPE=ADMISSION|TIME=2026-01-01T10:00:00Z|X=1"), model.ReasonStructureMismatch},
		{"duplicate field", textRec("MID=e|MID=e2|PID=p1|TYPE=ADMISSION|TIME=2026-01-01T10:00:00Z"), model.ReasonStructureMismatch},
		{"missing source", model.RawRecord{Kind: "text", Payload: []byte("MID=e|PID=p1|TYPE=ADMISSION|TIME=2026-01-01T10:00:00Z")}, model.ReasonMissingRequired},
		{"json syntax", jsonRec("{oops"), model.ReasonMalformed},
		{"json unknown field", jsonRec(`{"resourceType":"ClinicalEvent","recordId":"e","patientId":"p","eventType":"ADMISSION","eventTime":"2026-01-01T10:00:00Z","bogus":1}`), model.ReasonStructureMismatch},
		{"json wrong type", jsonRec(`{"resourceType":"ClinicalEvent","recordId":"e","patientId":"p","eventType":"ADMISSION","eventTime":123}`), model.ReasonStructureMismatch},
		{"json wrong resource", jsonRec(`{"resourceType":"Other","recordId":"e","patientId":"p","eventType":"ADMISSION","eventTime":"2026-01-01T10:00:00Z"}`), model.ReasonStructureMismatch},
		{"json missing field", jsonRec(`{"resourceType":"ClinicalEvent","recordId":"e","patientId":"p","eventType":"ADMISSION"}`), model.ReasonMissingRequired},
		{"json bad code", jsonRec(`{"resourceType":"ClinicalEvent","recordId":"e","patientId":"p","eventType":"WAT","eventTime":"2026-01-01T10:00:00Z"}`), model.ReasonInvalidCode},
		{"json bad time", jsonRec(`{"resourceType":"ClinicalEvent","recordId":"e","patientId":"p","eventType":"ADMISSION","eventTime":"yesterday"}`), model.ReasonStructureMismatch},
		{"json trailing data", jsonRec(`{"resourceType":"ClinicalEvent","recordId":"e","patientId":"p","eventType":"ADMISSION","eventTime":"2026-01-01T10:00:00Z"}junk`), model.ReasonMalformed},
		{"unknown kind", model.RawRecord{SourceSystem: "s", Kind: "xml", Payload: []byte("<x/>")}, model.ReasonMalformed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, rej := Parse(tc.rec)
			if rej == nil {
				t.Fatalf("%s: expected rejection %s, got acceptance", tc.name, tc.reason)
			}
			if rej.Reason != tc.reason {
				t.Fatalf("%s: reason=%s want %s (detail=%q)",
					tc.name, rej.Reason, tc.reason, rej.Detail)
			}
			t.Logf("%-22s -> reason=%-22s detail=%q", tc.name, rej.Reason, rej.Detail)
		})
	}
}

func TestParseJSONHappyPath(t *testing.T) {
	body := `{
	  "resourceType": "ClinicalEvent",
	  "recordId": "J1",
	  "patientId": "P 777",
	  "eventType": "DIAGNOSIS",
	  "eventTime": "2026-02-03T08:30:00+02:00",
	  "gender": "F"
	}`
	n, rej := Parse(jsonRec(body))
	if rej != nil {
		t.Fatalf("unexpected rejection: %s", rej.Detail)
	}
	t.Logf("json parsed: record=%s patient=%q type=%s time(UTC)=%s gender=%s",
		n.RecordID, n.PatientID, n.EventType, n.EventTime, n.Gender)
	if n.EventType != model.EventDiagnosis || n.Gender != model.GenderFemale {
		t.Fatal("json normalization wrong")
	}
	// +02:00 08:30 == 06:30 UTC
	if n.EventTime.Format("15:04") != "06:30" {
		t.Fatalf("time not normalized to UTC: %s", n.EventTime)
	}
}
