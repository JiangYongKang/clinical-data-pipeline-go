package source

import (
	"testing"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
)

func good(id string) string {
	return `{"resourceType":"Observation","id":"` + id + `","subject":"p","code":"HR",` +
		`"effectiveDateTime":"2026-09-01T10:00:00Z","value":"70"}`
}

func TestSplitMultiLineSingleObject(t *testing.T) {
	body := "{\n  \"resourceType\": \"Observation\",\n  \"id\": \"e1\",\n" +
		"  \"subject\": \"p\", \"code\": \"HR\",\n  \"effectiveDateTime\": " +
		"\"2026-09-01T10:00:00Z\", \"value\": \"70\"\n}"
	es := SplitJSONStream([]byte(body))
	if len(es) != 1 || es[0].Rejection != nil {
		t.Fatalf("want 1 clean entry, got %+v", es)
	}
	if rec, rej := ParseJSON(es[0].Data); rej != nil || rec.EventID != "e1" {
		t.Fatalf("multiline object must parse: rec=%+v rej=%+v", rec, rej)
	}
	t.Logf("多行单对象按边界切成 1 条并成功解析 event=%s", "e1")
}

func TestSplitArrayAndSequence(t *testing.T) {
	arr := "[" + good("a") + ",\n  " + good("b") + ", " + good("c") + "]"
	seq := good("a") + "\n" + good("b") + "," + good("c")
	for name, body := range map[string]string{"array": arr, "sequence": seq} {
		es := SplitJSONStream([]byte(body))
		if len(es) != 3 {
			t.Fatalf("%s: want 3 entries, got %d", name, len(es))
		}
		for i, e := range es {
			if e.Rejection != nil {
				t.Fatalf("%s[%d] unexpected lex rejection: %+v", name, i, e.Rejection)
			}
		}
		t.Logf("%s 形态切成 %d 个资源对象，缩进换行不影响边界", name, len(es))
	}
}

func TestSplitTruncatedOnlyKillsFragment(t *testing.T) {
	// 第一个对象完好，第二个写到一半被截断：前者仍可摄入，后者可区分拒绝。
	body := good("a") + ` {"resourceType":"Observation","id":"broken"`
	es := SplitJSONStream([]byte(body))
	if len(es) != 2 {
		t.Fatalf("want 2 entries, got %d", len(es))
	}
	if es[0].Rejection != nil {
		t.Fatalf("完好对象不应被拒绝: %+v", es[0].Rejection)
	}
	if es[1].Rejection == nil || es[1].Rejection.Reason != model.RejectMalformed {
		t.Fatalf("截断片段应 MALFORMED, got %+v", es[1].Rejection)
	}
	t.Logf("截断片段拒绝原因=%s detail=%s；同文件完好对象不受影响",
		es[1].Rejection.Reason, es[1].Rejection.Detail)
}

func TestSplitElementsStillStrictlyValidated(t *testing.T) {
	// 放宽排版不等于放宽结构：数组元素里多余字段/缺字段/坏编码仍各自可区分拒绝。
	cases := []struct {
		name   string
		body   string
		reason model.RejectReason
	}{
		{"unknown-field", `[` + good("a") + `,{"resourceType":"Observation","id":"x","subject":"p","code":"HR","effectiveDateTime":"2026-09-01T10:00:00Z","value":"1","extra":1}]`, model.RejectStructureMismatch},
		{"missing-field", `[{"resourceType":"Observation","subject":"p","code":"HR","effectiveDateTime":"2026-09-01T10:00:00Z","value":"1"}]`, model.RejectMissingField},
		{"bad-code", `[` + good("a") + `,{"resourceType":"Observation","id":"x","subject":"p","code":"NOPE","effectiveDateTime":"2026-09-01T10:00:00Z","value":"1"}]`, model.RejectInvalidCode},
		{"bad-time", `[{"resourceType":"Observation","id":"x","subject":"p","code":"HR","effectiveDateTime":"yesterday","value":"1"}]`, model.RejectStructureMismatch},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			es := SplitJSONStream([]byte(c.body))
			var saw bool
			for _, e := range es {
				if e.Rejection != nil {
					continue
				}
				_, rej := ParseJSON(e.Data)
				if rej != nil {
					saw = true
					t.Logf("%s -> reason=%s field=%s detail=%s", c.name, rej.Reason, rej.Field, rej.Detail)
					if rej.Reason != c.reason {
						t.Fatalf("reason=%s want %s", rej.Reason, c.reason)
					}
				}
			}
			if !saw {
				t.Fatalf("%s: 没有元素被按预期拒绝", c.name)
			}
		})
	}
}

func TestRecordSourceSystem(t *testing.T) {
	body := `{"resourceType":"Observation","sourceSystem":"HIS-A","id":"e1","subject":"p","code":"HR","effectiveDateTime":"2026-09-01T10:00:00Z","value":"70"}`
	rec, rej := ParseJSONWithSystem([]byte(body), "feed-default")
	if rej != nil {
		t.Fatalf("unexpected: %+v", rej)
	}
	if rec.SourceSystem != "HIS-A" {
		t.Fatalf("记录自带 sourceSystem 应优先，got %q", rec.SourceSystem)
	}
	plain := good("e2")
	rec2, _ := ParseJSONWithSystem([]byte(plain), "HIS-B")
	if rec2.SourceSystem != "HIS-B" {
		t.Fatalf("投递级缺省应生效, got %q", rec2.SourceSystem)
	}
	rec3, _ := ParseJSON([]byte(plain))
	if rec3.SourceSystem != DefaultSourceSystem {
		t.Fatalf("两者皆空应落到 %s, got %q", DefaultSourceSystem, rec3.SourceSystem)
	}
	t.Logf("来源系统解析：记录自带 > 投递级 > %s", DefaultSourceSystem)
}
