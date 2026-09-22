// Package identity 负责患者身份归一化、归并、冲突判定与去标识化映射。
package identity

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"sync"
	"unicode"
)

// Registry 是线程安全的患者身份注册表。
// 重复导入与并发导入同一患者必须得到同一去标识化 ID。
type Registry struct {
	mu      sync.Mutex
	byCanon map[string]string            // 规范患者键 -> 去标识化 ID
	attrs   map[string]map[string]string // 规范患者键 -> 已登记身份属性
}

// NewRegistry 创建空注册表。
func NewRegistry() *Registry {
	return &Registry{
		byCanon: make(map[string]string),
		attrs:   make(map[string]map[string]string),
	}
}

// Canonicalize 将来源患者标识归一化为规范键：
// 去除首尾空白、去掉内部所有空白与连字符/下划线等分隔符、统一为大写。
// 因此 "mrn-123"、" MRN 123 "、"mrn_123" 归并为同一键。
func Canonicalize(raw string) string {
	raw = strings.TrimSpace(raw)
	var b strings.Builder
	b.Grow(len(raw))
	for _, r := range raw {
		switch {
		case unicode.IsSpace(r), r == '-', r == '_':
			continue // 等价表示差异：忽略分隔符
		default:
			b.WriteRune(unicode.ToUpper(r))
		}
	}
	return b.String()
}

// identityAttrKeys 是参与冲突判定的身份属性键。
var identityAttrKeys = []string{"birthDate", "familyName"}

// deidentify 由规范键确定性推导去标识化 ID（可复现，无随机性）。
func deidentify(canonical string) string {
	sum := sha256.Sum256([]byte("deid-v1:" + canonical))
	return "PAT-" + strings.ToUpper(hex.EncodeToString(sum[:8]))
}

// Resolve 返回该患者的去标识化 ID。
// attrs 为身份属性（如 birthDate）；同一规范键若出现冲突属性则返回 ok=false，
// 调用方必须拒绝该记录而不是任选其一。
// 映射可复现：同一规范键永远得到同一去标识化 ID；并发与重复调用幂等。
func (r *Registry) Resolve(canonical string, attrs map[string]string) (deid string, ok bool) {
	if canonical == "" {
		return "", false
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	known, exists := r.attrs[canonical]
	if !exists {
		known = map[string]string{}
		r.attrs[canonical] = known
	}
	// 冲突判定：仅当双方都为同一身份属性提供了值且不一致时判冲突；
	// 一方未提供视为信息缺失，登记新值以收紧后续判定。
	for _, k := range identityAttrKeys {
		v, provided := attrs[k]
		if !provided || v == "" {
			continue
		}
		if kv, has := known[k]; has && kv != "" && kv != v {
			return "", false // 冲突：明确拒绝
		}
		known[k] = v
	}
	if id, has := r.byCanon[canonical]; has {
		return id, true
	}
	id := deidentify(canonical)
	r.byCanon[canonical] = id
	return id, true
}
