// 事务式身份登记、快照恢复、映射导出与显式反查。
//
// 批次处理期间身份登记只发生在事务副本上：批次任一条记录失败则整个
// 事务丢弃，注册表不留下“登记了一半”的痕迹；提交时与活注册表二次校验，
// 冲突则拒绝提交。
package identity

import (
	"errors"
	"sort"
)

// Snapshot 是注册表可持久化的完整状态快照。
type Snapshot struct {
	ByCanon map[string]string            `json:"byCanon"` // 规范患者键 -> 去标识化 ID
	Attrs   map[string]map[string]string `json:"attrs"`   // 规范患者键 -> 已登记身份属性
	RawID   map[string]string            `json:"rawId"`   // 规范患者键 -> 首次见到的原始患者标识（反查用）
}

// MappingEntry 是一条可导出的重标识映射（含可还原信息，须严格控制权限）。
type MappingEntry struct {
	CanonicalPID   string `json:"canonicalPid"`
	DeidentifiedID string `json:"deidentifiedId"`
	FirstRawID     string `json:"firstRawId,omitempty"`
}

// Snapshot 导出当前注册表的完整快照（深拷贝，调用方可自由修改）。
func (r *Registry) Snapshot() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return cloneSnapshotLocked(Snapshot{ByCanon: r.byCanon, Attrs: r.attrs, RawID: r.rawID})
}

// Restore 用快照替换注册表内容（启动恢复用；深拷贝避免别名共享）。
func (r *Registry) Restore(s Snapshot) {
	cp := cloneSnapshotLocked(s)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byCanon = cp.ByCanon
	r.attrs = cp.Attrs
	r.rawID = cp.RawID
}

func cloneSnapshotLocked(s Snapshot) Snapshot {
	by := make(map[string]string, len(s.ByCanon))
	for k, v := range s.ByCanon {
		by[k] = v
	}
	raw := make(map[string]string, len(s.RawID))
	for k, v := range s.RawID {
		raw[k] = v
	}
	at := make(map[string]map[string]string, len(s.Attrs))
	for k, v := range s.Attrs {
		c := make(map[string]string, len(v))
		for kk, vv := range v {
			c[kk] = vv
		}
		at[k] = c
	}
	return Snapshot{ByCanon: by, Attrs: at, RawID: raw}
}

// ExportMappings 按去标识化 ID 排序导出全部映射，供本地核对。
// 导出内容含可还原信息，调用方必须写入 0600 权限文件并妥善保管。
func (r *Registry) ExportMappings() []MappingEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]MappingEntry, 0, len(r.byCanon))
	for canon, deid := range r.byCanon {
		out = append(out, MappingEntry{
			CanonicalPID:   canon,
			DeidentifiedID: deid,
			FirstRawID:     r.rawID[canon],
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DeidentifiedID < out[j].DeidentifiedID })
	return out
}

// ReverseLookupByCanon 按规范患者键返回去标识化 ID 与首次登记的原始标识。
func (r *Registry) ReverseLookupByCanon(canonical string) (deid, rawID string, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id, has := r.byCanon[canonical]
	if !has {
		return "", "", false
	}
	return id, r.rawID[canonical], true
}

// ReverseLookup 按去标识化 ID 反查规范患者键与首次登记的原始患者标识。
// 该接口只用于本地排查，默认不开放给常规输出路径。
func (r *Registry) ReverseLookup(deid string) (canonical, rawID string, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for canon, id := range r.byCanon {
		if id == deid {
			return canon, r.rawID[canon], true
		}
	}
	return "", "", false
}

// ErrIdentityConflictAtCommit 表示事务提交时与活注册表存在身份冲突。
var ErrIdentityConflictAtCommit = errors.New("身份事务提交时检测到冲突")

// Tx 是基于注册表快照的暂存事务，结束前不触碰活注册表。
type Tx struct {
	reg     *Registry
	byCanon map[string]string
	attrs   map[string]map[string]string
	rawID   map[string]string
	touched map[string]struct{}
}

// Begin 基于当前快照开启一个登记事务。
func (r *Registry) Begin() *Tx {
	r.mu.Lock()
	snap := cloneSnapshotLocked(Snapshot{ByCanon: r.byCanon, Attrs: r.attrs, RawID: r.rawID})
	r.mu.Unlock()
	return &Tx{
		reg:     r,
		byCanon: snap.ByCanon,
		attrs:   snap.Attrs,
		rawID:   snap.RawID,
		touched: map[string]struct{}{},
	}
}

// Resolve 在事务副本上解析去标识化 ID，冲突规则与 Registry.Resolve 一致。
// rawID 为来源原始患者标识，仅在首次见到该规范键时记录（用于反查）。
func (t *Tx) Resolve(canonical, rawID string, attrs map[string]string) (deid string, ok bool) {
	if canonical == "" {
		return "", false
	}
	t.touched[canonical] = struct{}{}
	known, exists := t.attrs[canonical]
	if !exists {
		known = map[string]string{}
		t.attrs[canonical] = known
	}
	for _, k := range identityAttrKeys {
		v, provided := attrs[k]
		if !provided || v == "" {
			continue
		}
		if kv, has := known[k]; has && kv != "" && kv != v {
			return "", false
		}
		known[k] = v
	}
	if rawID != "" {
		if _, has := t.rawID[canonical]; !has {
			t.rawID[canonical] = rawID
		}
	}
	if id, has := t.byCanon[canonical]; has {
		return id, true
	}
	id := deidentify(canonical)
	t.byCanon[canonical] = id
	return id, true
}

// Snapshot 返回事务当前暂存内容的完整快照（供“先落盘后提交”使用）。
func (t *Tx) Snapshot() Snapshot {
	return cloneSnapshotLocked(Snapshot{ByCanon: t.byCanon, Attrs: t.attrs, RawID: t.rawID})
}

// Rollback 丢弃事务（Tx 是独立副本，丢弃即释放所有暂存登记）。
func (t *Tx) Rollback() {
	t.touched = map[string]struct{}{}
	t.byCanon = map[string]string{}
	t.attrs = map[string]map[string]string{}
	t.rawID = map[string]string{}
}

// Commit 将事务与活注册表做二次冲突校验后合并写入。
// 与并发登记冲突时返回 ErrIdentityConflictAtCommit，注册表保持不变。
func (t *Tx) Commit() error {
	r := t.reg
	r.mu.Lock()
	defer r.mu.Unlock()
	for canon := range t.touched {
		live := r.attrs[canon]
		for k, v := range t.attrs[canon] {
			if v == "" {
				continue
			}
			if lv, has := live[k]; has && lv != "" && lv != v {
				return ErrIdentityConflictAtCommit
			}
		}
	}
	for canon := range t.touched {
		work := t.attrs[canon]
		live, exists := r.attrs[canon]
		if !exists {
			r.attrs[canon] = work
		} else {
			for k, v := range work {
				if v != "" {
					live[k] = v
				}
			}
		}
		if id, has := t.byCanon[canon]; has {
			r.byCanon[canon] = id
		}
		if id, has := t.rawID[canon]; has {
			if _, exists := r.rawID[canon]; !exists {
				r.rawID[canon] = id
			}
		}
	}
	return nil
}
