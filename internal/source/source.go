// Package source 解析两类来源（结构化文本、资源型 JSON）为中间记录。
// 必填字段缺失、编码非法或结构与声明不符时返回可区分的 Rejection，绝不静默吞掉。
package source

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
)

// allowedCodes 是合法编码白名单（按编码系统约定）。
var allowedCodes = map[string]bool{
	"HR":     true, // 心率
	"BP-SYS": true, // 收缩压
	"BP-DIA": true, // 舒张压
	"TEMP":   true, // 体温
	"SPO2":   true, // 血氧
}

const maxRawHint = 80

func rawHint(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > maxRawHint {
		return s[:maxRawHint] + "..."
	}
	return s
}

func reject(reason model.RejectReason, field, detail, raw string) *model.Rejection {
	return &model.Rejection{Reason: reason, Field: field, Detail: detail, RawHint: rawHint(raw)}
}

// validateCommon 校验解析后的公共必填字段与编码合法性。
func validateCommon(rec *model.RawRecord, raw string) *model.Rejection {
	if rec.EventID == "" {
		return reject(model.RejectMissingField, "eventId", "必填字段 eventId 缺失", raw)
	}
	if rec.PatientID == "" {
		return reject(model.RejectMissingField, "patientId", "必填字段 patientId 缺失", raw)
	}
	if rec.EventType == "" {
		return reject(model.RejectMissingField, "eventType", "必填字段 eventType 缺失", raw)
	}
	if rec.Code == "" {
		return reject(model.RejectMissingField, "code", "必填字段 code 缺失", raw)
	}
	if !allowedCodes[rec.Code] {
		return reject(model.RejectInvalidCode, "code",
			fmt.Sprintf("编码 %q 不在合法编码表内", rec.Code), raw)
	}
	if rec.OccurredAt.IsZero() {
		return reject(model.RejectMissingField, "occurredAt", "必填字段 occurredAt 缺失或无法解析", raw)
	}
	return nil
}

// ParseText 解析结构化文本消息（管道分隔）。
// 格式: EVT|<eventID>|<patientID>|<eventType>|<code>|<RFC3339时间>|<值>
func ParseText(msg string) (model.RawRecord, *model.Rejection) {
	raw := msg
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return model.RawRecord{}, reject(model.RejectMalformed, "", "空消息", raw)
	}
	fields := strings.Split(msg, "|")
	if fields[0] != "EVT" {
		return model.RawRecord{}, reject(model.RejectStructureMismatch, "",
			fmt.Sprintf("声明的消息类型 %q 不是 EVT", fields[0]), raw)
	}
	if len(fields) != 7 {
		return model.RawRecord{}, reject(model.RejectStructureMismatch, "",
			fmt.Sprintf("EVT 消息应为 7 段，实际 %d 段", len(fields)), raw)
	}
	for i := range fields {
		fields[i] = strings.TrimSpace(fields[i])
	}
	rec := model.RawRecord{
		Source:    model.SourceText,
		EventID:   fields[1],
		PatientID: fields[2],
		EventType: fields[3],
		Code:      fields[4],
		Value:     fields[6],
		Attrs:     map[string]string{},
	}
	if fields[5] != "" {
		t, err := time.Parse(time.RFC3339, fields[5])
		if err != nil {
			return model.RawRecord{}, reject(model.RejectStructureMismatch, "occurredAt",
				fmt.Sprintf("时间 %q 不是合法 RFC3339", fields[5]), raw)
		}
		rec.OccurredAt = t.UTC()
	}
	if r := validateCommon(&rec, raw); r != nil {
		return model.RawRecord{}, r
	}
	return rec, nil
}

// observationResource 是资源型 JSON 的声明结构。
type observationResource struct {
	ResourceType string            `json:"resourceType"`
	ID           string            `json:"id"`
	Subject      string            `json:"subject"`
	Code         string            `json:"code"`
	Effective    string            `json:"effectiveDateTime"`
	Value        string            `json:"value"`
	Attrs        map[string]string `json:"attrs"`
}

// ParseJSON 解析资源型 JSON。
// 声明结构: {"resourceType":"Observation","id":...,"subject":...,"code":...,
// "effectiveDateTime":...,"value":...,"attrs":{...}}
func ParseJSON(data []byte) (model.RawRecord, *model.Rejection) {
	raw := string(data)
	var res observationResource
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields() // 结构与声明不符（多余字段）即拒绝
	if err := dec.Decode(&res); err != nil {
		return model.RawRecord{}, reject(model.RejectStructureMismatch, "",
			fmt.Sprintf("JSON 结构与声明不符: %v", err), raw)
	}
	if res.ResourceType != "Observation" {
		return model.RawRecord{}, reject(model.RejectStructureMismatch, "resourceType",
			fmt.Sprintf("resourceType %q 不是 Observation", res.ResourceType), raw)
	}
	rec := model.RawRecord{
		Source:    model.SourceJSON,
		EventID:   res.ID,
		PatientID: res.Subject,
		EventType: "observation",
		Code:      res.Code,
		Value:     res.Value,
		Attrs:     res.Attrs,
	}
	if rec.Attrs == nil {
		rec.Attrs = map[string]string{}
	}
	if res.Effective != "" {
		t, err := time.Parse(time.RFC3339, res.Effective)
		if err != nil {
			return model.RawRecord{}, reject(model.RejectStructureMismatch, "effectiveDateTime",
				fmt.Sprintf("时间 %q 不是合法 RFC3339", res.Effective), raw)
		}
		rec.OccurredAt = t.UTC()
	}
	if r := validateCommon(&rec, raw); r != nil {
		return model.RawRecord{}, r
	}
	return rec, nil
}
