// Package source 解析两类来源（结构化文本、资源型 JSON）为中间记录。
// 必填字段缺失、编码非法或结构与声明不符时返回可区分的 Rejection，绝不静默吞掉。
package source

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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
// 格式: EVT|<eventID>|<patientID>|<eventType>|<code>|<RFC3339时间>|<值>[|<来源系统>]
// 第 8 段来源系统可选；消息内未携带时使用调用方给出的 defaultSystem。
func ParseText(msg string, defaultSystem string) (model.RawRecord, *model.Rejection) {
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
	if len(fields) != 7 && len(fields) != 8 {
		return model.RawRecord{}, reject(model.RejectStructureMismatch, "",
			fmt.Sprintf("EVT 消息应为 7 或 8 段，实际 %d 段", len(fields)), raw)
	}
	for i := range fields {
		fields[i] = strings.TrimSpace(fields[i])
	}
	system := defaultSystem
	if len(fields) == 8 && fields[7] != "" {
		system = fields[7] // 消息内显式携带的来源系统优先
	}
	rec := model.RawRecord{
		Source:       model.SourceText,
		SourceSystem: system,
		EventID:      fields[1],
		PatientID:    fields[2],
		EventType:    fields[3],
		Code:         fields[4],
		Value:        fields[6],
		Attrs:        map[string]string{},
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
	SourceSystem string            `json:"sourceSystem"`
	Attrs        map[string]string `json:"attrs"`
}

// ParseJSON 解析单个资源型 JSON 对象。
// 声明结构: {"resourceType":"Observation","id":...,"subject":...,"code":...,
// "effectiveDateTime":...,"value":...,"sourceSystem":...,"attrs":{...}}
// 对象内未携带 sourceSystem 时使用调用方给出的 defaultSystem。
func ParseJSON(data []byte, defaultSystem string) (model.RawRecord, *model.Rejection) {
	raw := string(data)
	var res observationResource
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields() // 结构与声明不符（多余字段）即拒绝
	if err := dec.Decode(&res); err != nil {
		return model.RawRecord{}, reject(model.RejectStructureMismatch, "",
			fmt.Sprintf("JSON 结构与声明不符: %v", err), raw)
	}
	// 不允许同一对象后尾随其它 JSON 值：一个对象的边界必须清晰。
	if tok, err := dec.Token(); err != io.EOF {
		if err != nil {
			return model.RawRecord{}, reject(model.RejectStructureMismatch, "",
				fmt.Sprintf("JSON 对象后存在非法尾随内容: %v", err), raw)
		}
		return model.RawRecord{}, reject(model.RejectStructureMismatch, "",
			fmt.Sprintf("JSON 对象后尾随额外数据 %v，必须一对象一记录", tok), raw)
	}
	if res.ResourceType != "Observation" {
		return model.RawRecord{}, reject(model.RejectStructureMismatch, "resourceType",
			fmt.Sprintf("resourceType %q 不是 Observation", res.ResourceType), raw)
	}
	system := defaultSystem
	if strings.TrimSpace(res.SourceSystem) != "" {
		system = strings.TrimSpace(res.SourceSystem) // 对象内显式携带的来源系统优先
	}
	rec := model.RawRecord{
		Source:       model.SourceJSON,
		SourceSystem: system,
		EventID:      res.ID,
		PatientID:    res.Subject,
		EventType:    "observation",
		Code:         res.Code,
		Value:        res.Value,
		Attrs:        res.Attrs,
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

// SplitJSONObjectStream 按资源对象的边界切分一个 JSON 文件的字节。
// 允许的顶层形态（与缩进、换行无关）：
//   - 单个对象：{...}
//   - 多个对象并排：{...}{...}
//   - 一个数组，元素为对象：[{...},{...}]
//
// 返回每个对象的原始 JSON 字节，交给 ParseJSON 逐个做严格字段校验，
// 因此多余字段、缺字段、非法时间/编码仍逐条给出可区分拒绝，不会因排版放宽而被吞掉。
// 顶层结构本身不合法（数组元素不是对象、出现裸值、截断、尾随垃圾等）时返回错误。
func SplitJSONObjectStream(data []byte) ([][]byte, error) {
	first := skipJSONSpace(data, 0)
	if first == len(data) {
		return nil, nil // 空文件：零条记录
	}
	switch data[first] {
	case '[':
		return splitJSONArray(data, first)
	case '{':
		return splitJSONObjects(data, first)
	default:
		return nil, fmt.Errorf("JSON 顶层必须是对象或数组，实际字节为 %q", data[first])
	}
}

// splitJSONObjects 从 start（指向第一个 '{'）起切出一个或多个并排对象。
func splitJSONObjects(data []byte, start int) ([][]byte, error) {
	var out [][]byte
	i := start
	for {
		end, err := scanJSONObject(data, i)
		if err != nil {
			return nil, err
		}
		out = append(out, data[i:end])
		i = skipJSONSpace(data, end)
		if i == len(data) {
			return out, nil
		}
		if data[i] != '{' {
			return nil, fmt.Errorf("并排 JSON 值之间存在非法字节 %q", data[i])
		}
	}
}

// splitJSONArray 校验顶层数组并切出其中的对象元素。
func splitJSONArray(data []byte, start int) ([][]byte, error) {
	var arr []json.RawMessage
	if err := json.Unmarshal(data[start:], &arr); err != nil {
		return nil, fmt.Errorf("JSON 数组无法解析: %w", err)
	}
	out := make([][]byte, 0, len(arr))
	for i, raw := range arr {
		trimmed := bytes.TrimSpace(raw)
		if len(trimmed) == 0 || trimmed[0] != '{' {
			return nil, fmt.Errorf("JSON 数组第 %d 个元素不是资源对象", i)
		}
		// 重新编码为紧凑形式，消除元素间空白，保留全部字段内容。
		var buf bytes.Buffer
		if err := json.Compact(&buf, trimmed); err != nil {
			return nil, fmt.Errorf("JSON 数组第 %d 个元素无法解析: %w", i, err)
		}
		out = append(out, buf.Bytes())
	}
	return out, nil
}

// scanJSONObject 从 data[start] == '{' 起扫描，返回对象结束后的偏移（已越过 '}'）。
// 正确处理字符串内的括号、转义符与嵌套结构；对象截断或缺引号即报错。
func scanJSONObject(data []byte, start int) (int, error) {
	depth := 0
	inString := false
	escaped := false
	for i := start; i < len(data); i++ {
		c := data[i]
		if inString {
			switch {
			case escaped:
				escaped = false
			case c == '\\':
				escaped = true
			case c == '"':
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i + 1, nil
			}
		}
	}
	return 0, fmt.Errorf("JSON 对象在文件结束前未闭合")
}

// skipJSONSpace 返回从 i 起第一个非空白字节偏移。
func skipJSONSpace(data []byte, i int) int {
	for i < len(data) {
		switch data[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}
