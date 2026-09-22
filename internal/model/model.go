// Package model 定义管线统一数据模型、拒绝原因与数据质量判定。
package model

import "time"

// SourceType 标识来源类型。
type SourceType string

const (
	SourceText SourceType = "structured-text" // 结构化文本消息（管道分隔）
	SourceJSON SourceType = "resource-json"   // 资源型 JSON
)

// RejectReason 是可区分的拒绝原因。
type RejectReason string

const (
	RejectMissingField      RejectReason = "MISSING_FIELD"       // 必填字段缺失
	RejectInvalidCode       RejectReason = "INVALID_CODE"        // 编码取值非法
	RejectStructureMismatch RejectReason = "STRUCTURE_MISMATCH"  // 结构与声明不符
	RejectIdentityConflict  RejectReason = "IDENTITY_CONFLICT"   // 身份冲突且无法判定
	RejectMalformed         RejectReason = "MALFORMED"           // 无法解析的输入
)

// Rejection 描述一条记录被拒绝的完整判定依据。
type Rejection struct {
	Reason  RejectReason
	Field   string // 相关字段（可为空）
	Detail  string // 判定依据说明
	RawHint string // 原始记录摘要（截断）
}

// Quality 是数据质量判定结果。
type Quality struct {
	Accepted  bool
	Rejection *Rejection // Accepted=false 时非空
}

// RawRecord 是解析后、归一化前的中间记录。
type RawRecord struct {
	Source     SourceType
	EventID    string
	PatientID  string // 来源原始患者标识
	EventType  string
	Code       string // 观察/事件编码
	OccurredAt time.Time
	Value      string
	Attrs      map[string]string // 额外属性（如出生日期等身份属性）
}

// NormalizedEvent 是归一化后的统一事件模型。
type NormalizedEvent struct {
	EventID        string
	CanonicalPID   string // 归并后的规范患者键（内部使用）
	DeidentifiedID string // 去标识化患者 ID（对外输出）
	EventType      string
	Code           string
	OccurredAt     time.Time
	Value          string
}

// Outcome 是单条记录的处理结论。
type Outcome struct {
	Event   *NormalizedEvent // 接受时非空
	Quality Quality
}
