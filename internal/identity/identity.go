// Package identity 负责患者身份归一化、归并、冲突判定与去标识化映射。
package identity

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strings"
	"sync"
	"unicode"
)

// Record 是一条持久化的身份登记记录。
type Record struct {
	Canonical    string            `json:"canonical"`            // 规范患者键
	Deidentified string            `json:"deidentified"`         // 去标识化 ID
	RawExample   string            `json:"rawExample,omitempty"` // 首次见到的原始患者标识（仅供本地反查）
	Attrs        map[string]string `json:"attrs,omitempty"`      // 已登记身份属性
}

// Snapshot 是注册表的可持久化快照。
type Snapshot struct {
	Records []Record `json:"records"`
}

// Registry 是线程安全的患者身份注册表。
// 重复导入与并发导入同一患者必须得到同一去标识化 ID。
type Registry struct {
	mu      sync.Mutex
	byCanon map[string]*Record // 规范患者键 -> 登记记录
}

// NewRegistry 创建空注册表。
func NewRegistry() *Registry {
	return &Registry{byCanon: make(map[string]*Record)}
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

// sortStrings 是 sort.Strings 的本地别名，集中表达“确定性输出”意图。
func sortStrings(s []string) { sort.Strings(s) }

// errCorrupt 构造持久化数据损坏错误。
func errCorrupt(msg string) error { return errors.New("身份快照损坏: " + msg) }

// deidentify 由规范键确定性推导去标识化 ID（可复现，无随机性）。
func deidentify(canonical string) string {
	sum := sha256.Sum256([]byte("deid-v1:" + canonical))
	return "PAT-" + strings.ToUpper(hex.EncodeToString(sum[:8]))
}

// Tx 是一次批次内的身份暂存视图。
// 解析阶段所有解析与冲突判定只读写暂存，绝不触碰已提交注册表；
// 只有批次整体成功时 Commit 才合并落盘，失败时 Abort 丢弃，
// 因而被回滚的批次不会在身份登记里留下“只做了一半”的痕迹。
type Tx struct {
	reg    *Registry
	staged map[string]*Record // 规范键 -> 本批候选登记（含合并后的属性）
}

// Begin 开启一次身份暂存事务。
func (r *Registry) Begin() *Tx {
	return &Tx{reg: r, staged: make(map[string]*Record)}
}

// Resolve 在事务内返回该患者的去标识化 ID。
// rawPatientID 是来源原始患者标识，仅在首次登记该规范键时留存一份，
// 用于显式开启的本地反查；attrs 为参与冲突判定的身份属性。
// 同一规范键属性冲突时 ok=false，调用方必须拒绝该记录而不是任选其一。
func (tx *Tx) Resolve(canonical, rawPatientID string, attrs map[string]string) (deid string, ok bool) {
	if canonical == "" {
		return "", false
	}
	tx.reg.mu.Lock()
	defer tx.reg.mu.Unlock()

	// 生效视图 = 已提交记录叠加本批暂存。
	eff := &Record{Canonical: canonical, Attrs: map[string]string{}}
	if committed, has := tx.reg.byCanon[canonical]; has {
		eff.Deidentified = committed.Deidentified
		eff.RawExample = committed.RawExample
		for k, v := range committed.Attrs {
			eff.Attrs[k] = v
		}
	}
	if cand, has := tx.staged[canonical]; has {
		eff = cand
	}
	if eff.Attrs == nil {
		eff.Attrs = map[string]string{}
	}

	// 冲突判定：仅当双方都为同一身份属性提供了值且不一致时判冲突；
	// 一方未提供视为信息缺失，登记新值以收紧后续判定。
	for _, k := range identityAttrKeys {
		v, provided := attrs[k]
		if !provided || v == "" {
			continue
		}
		if kv, has := eff.Attrs[k]; has && kv != "" && kv != v {
			return "", false // 冲突：明确拒绝，暂存不写入冲突值
		}
		eff.Attrs[k] = v
	}
	if eff.Deidentified == "" {
		eff.Deidentified = deidentify(canonical)
	}
	if eff.RawExample == "" {
		eff.RawExample = strings.TrimSpace(rawPatientID)
	}
	cp := *eff
	cp.Attrs = make(map[string]string, len(eff.Attrs))
	for k, v := range eff.Attrs {
		cp.Attrs[k] = v
	}
	tx.staged[canonical] = &cp
	return eff.Deidentified, true
}

// Commit 把暂存合并进注册表。幂等：重复提交同一内容不产生多余身份。
func (r *Registry) Commit(tx *Tx) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for canonical, cand := range tx.staged {
		committed, has := r.byCanon[canonical]
		if !has {
			stored := *cand
			r.byCanon[canonical] = &stored
			continue
		}
		for k, v := range cand.Attrs {
			if _, exists := committed.Attrs[k]; !exists {
				committed.Attrs[k] = v
			}
		}
		if committed.RawExample == "" {
			committed.RawExample = cand.RawExample
		}
	}
}

// Abort 丢弃本批暂存（被回滚批次不留下任何痕迹）。
func (tx *Tx) Abort() { tx.staged = map[string]*Record{} }

// Resolve 是无显式事务时的便捷封装：解析并立即提交。
// 语义与“Begin -> Resolve -> Commit”一致，供进程内一次性调用与既有测试使用。
func (r *Registry) Resolve(canonical string, attrs map[string]string) (deid string, ok bool) {
	tx := r.Begin()
	id, ok := tx.Resolve(canonical, "", attrs)
	if !ok {
		return "", false
	}
	r.Commit(tx)
	return id, true
}

// ExportSnapshot 返回注册表的确定性快照（按规范键排序），用于持久化与导出。
func (r *Registry) ExportSnapshot() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return exportLocked(r.byCanon)
}

// SnapshotWithTx 返回“已提交记录叠加本批暂存”的确定性快照，但不修改注册表。
// 用于“先落盘后承认提交”：落盘成功后再 Commit，保证崩溃窗口内不留半成品。
func (r *Registry) SnapshotWithTx(tx *Tx) Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	merged := make(map[string]*Record, len(r.byCanon)+len(tx.staged))
	for k, rec := range r.byCanon {
		cp := *rec
		cp.Attrs = make(map[string]string, len(rec.Attrs))
		for ak, av := range rec.Attrs {
			cp.Attrs[ak] = av
		}
		merged[k] = &cp
	}
	for k, cand := range tx.staged {
		base, has := merged[k]
		if !has {
			cp := *cand
			cp.Attrs = make(map[string]string, len(cand.Attrs))
			for ak, av := range cand.Attrs {
				cp.Attrs[ak] = av
			}
			merged[k] = &cp
			continue
		}
		for ak, av := range cand.Attrs {
			if _, exists := base.Attrs[ak]; !exists {
				base.Attrs[ak] = av
			}
		}
		if base.RawExample == "" {
			base.RawExample = cand.RawExample
		}
	}
	return exportLocked(merged)
}

// exportLocked 从记录映射构造按规范键排序的快照；调用方需持有 r.mu。
func exportLocked(m map[string]*Record) Snapshot {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sortStrings(keys)
	snap := Snapshot{Records: make([]Record, 0, len(keys))}
	for _, k := range keys {
		rec := m[k]
		attrs := make(map[string]string, len(rec.Attrs))
		for ak, av := range rec.Attrs {
			attrs[ak] = av
		}
		snap.Records = append(snap.Records, Record{
			Canonical:    rec.Canonical,
			Deidentified: rec.Deidentified,
			RawExample:   rec.RawExample,
			Attrs:        attrs,
		})
	}
	return snap
}

// LoadSnapshot 从快照重建注册表（仅启动时调用）。
// 去标识化 ID 必须与规范键的确定性推导一致，否则视为损坏/被篡改并拒绝载入。
func (r *Registry) LoadSnapshot(snap Snapshot) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := make(map[string]*Record, len(snap.Records))
	for _, rec := range snap.Records {
		if rec.Canonical == "" {
			return errCorrupt("身份快照含空规范键")
		}
		if want := deidentify(rec.Canonical); rec.Deidentified != want {
			return errCorrupt("规范键 " + rec.Canonical + " 的去标识化编号与推导值不一致")
		}
		if dup, has := m[rec.Canonical]; has {
			_ = dup
			return errCorrupt("身份快照含重复规范键 " + rec.Canonical)
		}
		attrs := map[string]string{}
		for k, v := range rec.Attrs {
			attrs[k] = v
		}
		stored := rec
		stored.Attrs = attrs
		m[rec.Canonical] = &stored
	}
	r.byCanon = m
	return nil
}

// Reidentify 按去标识化编号反查规范键与首次留存的原始患者标识。
// 这是带可还原信息的本地排查能力，调用方必须在显式开启时才能使用。
func (r *Registry) Reidentify(deid string) (canonical, rawExample string, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rec := range r.byCanon {
		if rec.Deidentified == deid {
			return rec.Canonical, rec.RawExample, true
		}
	}
	return "", "", false
}

// Count 返回已登记患者数。
func (r *Registry) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.byCanon)
}
