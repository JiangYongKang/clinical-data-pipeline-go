package source

import (
	"testing"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
)

func TestParseTextValid(t *testing.T) {
	rec, rej := ParseText("EVT|e1|MRN-123|observation|HR|2026-09-01T10:00:00Z|72", "sys-default")
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
		{"wrong-field-count-nine", "EVT|e1|p1|observation|HR|2026-09-01T10:00:00Z|72|sys|x", model.RejectStructureMismatch},
		{"missing-event-id", "EVT||p1|observation|HR|2026-09-01T10:00:00Z|72", model.RejectMissingField},
		{"missing-patient", "EVT|e1||observation|HR|2026-09-01T10:00:00Z|72", model.RejectMissingField},
		{"missing-time", "EVT|e1|p1|observation|HR||72", model.RejectMissingField},
		{"bad-time", "EVT|e1|p1|observation|HR|not-a-time|72", model.RejectStructureMismatch},
		{"bad-code", "EVT|e1|p1|observation|GLUCOSE|2026-09-01T10:00:00Z|5.5", model.RejectInvalidCode},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, rej := ParseText(c.msg, "sys")
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
	rec, rej := ParseJSON([]byte(body), "sys-default")
	if rej != nil {
		t.Fatalf("unexpected rejection: %+v", rej)
	}
	t.Logf("event=%s patient=%s attrs=%v -> accepted", rec.EventID, rec.PatientID, rec.Attrs)
	if rec.EventID != "e9" || rec.PatientID != "mrn 123" || rec.Attrs["birthDate"] != "1990-01-01" {
		t.Fatalf("bad record: %+v", rec)
	}
	if rec.SourceSystem != "sys-default" {
		t.Fatalf("default source system not applied: %q", rec.SourceSystem)
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
			_, rej := ParseJSON([]byte(c.body), "sys")
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

func validObj(id string) string {
	return `{"resourceType":"Observation","id":"` + id + `","subject":"p1","code":"HR",` +
		`"effectiveDateTime":"2026-09-01T00:00:00Z","value":"1"}`
}

func TestSplitStreamShapes(t *testing.T) {
	cases := []struct {
		name  string
		input string
		n     int
	}{
		{"single-one-line", validObj("e1"), 1},
		{"single-pretty-multiline", "{\n  \"resourceType\": \"Observation\",\n  \"id\": \"e1\",\n" +
			"  \"subject\": \"p1\",\n  \"code\": \"HR\",\n" +
			"  \"effectiveDateTime\": \"2026-09-01T00:00:00Z\",\n  \"value\": \"1\"\n}\n", 1},
		{"two-concatenated", validObj("e1") + validObj("e2"), 2},
		{"two-concatenated-newline", validObj("e1") + "\n" + validObj("e2"), 2},
		{"array-two", `[` + validObj("e1") + `,` + validObj("e2") + `]`, 2},
		{"array-pretty", "[\n " + validObj("e1") + ",\n " + validObj("e2") + "\n]\n", 2},
		{"empty-file", "   \n", 0},
		{"empty-array", "[]", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			parts, err := SplitJSONObjectStream([]byte(c.input))
			if err != nil {
				t.Fatalf("split error: %v", err)
			}
			if len(parts) != c.n {
				t.Fatalf("got %d objects, want %d", len(parts), c.n)
			}
			for i, p := range parts {
				if _, rej := ParseJSON(p, "sys"); rej != nil {
					t.Fatalf("object %d parse rejected: %s %s", i, rej.Reason, rej.Detail)
				}
			}
			t.Logf("%s -> %d objects parsed by object boundary", c.name, len(parts))
		})
	}
}

func TestSplitStreamBadTopLevel(t *testing.T) {
	bad := []string{
		`"scalar"`,
		`[1,2,3]`,
		`[` + validObj("e1") + `, 2]`,
		validObj("e1") + ` 123`,
		validObj("e1")[:len(validObj("e1"))-1], // 截断对象（缺结尾花括号）
	}
	for i, in := range bad {
		if _, err := SplitJSONObjectStream([]byte(in)); err == nil {
			t.Fatalf("case %d expected split error, input=%s", i, in)
		} else {
			t.Logf("case %d rejected at stream level: %v", i, err)
		}
	}
}

func TestParseJSONObjectStreamPerRecordRejections(t *testing.T) {
	// 排版放宽不能吞掉逐条错误：多对象文件里坏一条、好一条，原因必须各自可区分。
	multi := `[
		{"resourceType":"Observation","id":"good","subject":"p1","code":"HR","effectiveDateTime":"2026-09-01T00:00:00Z","value":"1"},
		{"resourceType":"Observation","id":"bad","subject":"p1","code":"HR","effectiveDateTime":"2026-09-01T00:00:00Z","value":"1","extra":1},
		{"resourceType":"Observation","subject":"p1","code":"HR","effectiveDateTime":"2026-09-01T00:00:00Z","value":"1"},
		{"resourceType":"Observation","id":"badcode","subject":"p1","code":"XX","effectiveDateTime":"2026-09-01T00:00:00Z","value":"1"},
		{"resourceType":"Observation","id":"badtime","subject":"p1","code":"HR","effectiveDateTime":"nope","value":"1"}
	]`
	parts, err := SplitJSONObjectStream([]byte(multi))
	if err != nil || len(parts) != 5 {
		t.Fatalf("split: n=%d err=%v", len(parts), err)
	}
	want := []model.RejectReason{"", model.RejectStructureMismatch,
		model.RejectMissingField, model.RejectInvalidCode, model.RejectStructureMismatch}
	for i, p := range parts {
		_, rej := ParseJSON(p, "sys")
		if want[i] == "" {
			if rej != nil {
				t.Fatalf("obj %d unexpectedly rejected: %s", i, rej.Reason)
			}
			continue
		}
		if rej == nil || rej.Reason != want[i] {
			got := model.RejectReason("")
			if rej != nil {
				got = rej.Reason
			}
			t.Fatalf("obj %d reason=%s want %s", i, got, want[i])
		}
		t.Logf("obj %d -> %s field=%s", i, rej.Reason, rej.Field)
	}
}

func TestParseJSONSourceSystemPrecedence(t *testing.T) {
	body := `{"resourceType":"Observation","id":"e1","subject":"p1","code":"HR",` +
		`"effectiveDateTime":"2026-09-01T00:00:00Z","value":"1","sourceSystem":"HIS-A"}`
	rec, rej := ParseJSON([]byte(body), "default")
	if rej != nil {
		t.Fatalf("rejected: %+v", rej)
	}
	if rec.SourceSystem != "HIS-A" {
		t.Fatalf("explicit sourceSystem must win, got %q", rec.SourceSystem)
	}
	rec2, _ := ParseJSON([]byte(validObj("e1")), "HIS-B")
	if rec2.SourceSystem != "HIS-B" {
		t.Fatalf("default sourceSystem should apply when absent, got %q", rec2.SourceSystem)
	}
	// 文本第 8 段来源系统优先于默认值
	rec3, rej := ParseText("EVT|e1|p1|observation|HR|2026-09-01T00:00:00Z|1|HIS-C", "default")
	if rej != nil || rec3.SourceSystem != "HIS-C" {
		t.Fatalf("8th-field system not applied: rec=%+v rej=%+v", rec3, rej)
	}
}
