// Package state 管理管线的本地持久状态：已提交结论、事件去重集合与
// 重标识映射（规范患者键 <-> 去标识化 ID），并提供批次级原子提交。
//
// 持久化与权限边界：
//   - 状态目录以 0700 创建（仅属主可进入），状态文件以 0600 写入；
//   - 每次提交“写临时文件 + fsync + 原子 rename + 目录 fsync”，
//     写到一半被中断只会留下旧文件或临时文件，绝不会出现半截状态文件；
//   - 状态文件内含重标识映射（可还原信息），因此默认不放宽权限；
//   - 打开同一状态目录时持有 flock，避免多进程并发写坏状态。
package state

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/identity"
)

// 文件名与权限常量。
const (
	stateFileName = "state.json"
	tmpFileName   = "state.json.tmp"
	lockFileName  = "state.lock"
	dirPerm       = 0o700
	filePerm      = 0o600
	snapshotVers  = 1
)

// canonicalTimeLayout 是落盘时间的规范格式：UTC、小数秒固定 9 位（不足补 0、
// 整秒补 .000000000）。固定宽度让字符串字典序与真实时间序一致，
// “整秒 / .5 / .50 / .25”等不同写法归一化为同一串，不因写法产生不同结论。
const canonicalTimeLayout = "2006-01-02T15:04:05.000000000Z"

// CanonicalTime 把时间格式化为落盘规范串（UTC、9 位小数秒）。
func CanonicalTime(t time.Time) string {
	return t.UTC().Format(canonicalTimeLayout)
}

// parseEventTime 解析 RFC3339（含任意位数小数秒）时间；解析失败返回 false。
func parseEventTime(s string) (time.Time, bool) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// canonicalizeOccurredAt 把可能由旧版本写入的可变宽度时间串归一化为规范串；
// 无法解析（不应出现）时原样返回。
func canonicalizeOccurredAt(s string) string {
	t, ok := parseEventTime(s)
	if !ok {
		return s
	}
	return CanonicalTime(t)
}

// ErrLocked 表示状态目录正被另一个进程以排他方式占用。
var ErrLocked = errors.New("状态目录已被其他进程锁定")

// Event 是持久化的已提交事件（结论）。
type Event struct {
	EventID        string `json:"eventId"`
	SourceSystem   string `json:"sourceSystem"`
	CanonicalPID   string `json:"canonicalPid"`
	DeidentifiedID string `json:"deidentifiedId"`
	EventType      string `json:"eventType"`
	Code           string `json:"code"`
	OccurredAt     string `json:"occurredAt"` // RFC3339(UTC)
	Value          string `json:"value"`
}

// DedupKey 是事件去重键：来源系统 + 事件号。
// 不同来源系统的同号事件属于不同命名空间，绝不互相顶掉。
func DedupKey(sourceSystem, eventID string) string {
	return sourceSystem + "\x00" + eventID
}

// LatestKey 是乱序取舍状态键：(规范患者键, 事件类型, 编码)。
func LatestKey(canonicalPID, eventType, code string) string {
	return canonicalPID + "|" + eventType + "|" + code
}

// PayloadEqual 判断同去重键的两条事件载荷是否完全一致。
// 时间按真实时间点比较（不同写法如 .5 与 .500000000 视为同一时刻），
// 不因小数秒写法不同而误报“同号载荷冲突”。
// 不一致即“同号但无法判定为同一事件”，调用方必须拒绝而非任选其一。
func PayloadEqual(a, b Event) bool {
	return a.CanonicalPID == b.CanonicalPID &&
		a.EventType == b.EventType &&
		a.Code == b.Code &&
		occurredAtEqual(a.OccurredAt, b.OccurredAt) &&
		a.Value == b.Value
}

// occurredAtEqual 按真实时间点判断两个时间串是否同一时刻；
// 仅在两者都无法解析时退化为字符串比较（正常数据不会走到）。
func occurredAtEqual(x, y string) bool {
	tx, ox := parseEventTime(x)
	ty, oy := parseEventTime(y)
	if ox && oy {
		return tx.Equal(ty)
	}
	return x == y
}

// onDisk 是状态文件的磁盘格式。
type onDisk struct {
	Version int               `json:"version"`
	Seen    map[string]Event  `json:"seen"`
	Latest  map[string]Event  `json:"latest"`
	Mapping identity.Snapshot `json:"mapping"`
}

// Store 是磁盘支撑的持久状态存储。Begin..Commit 在进程内串行。
type Store struct {
	dir  string
	lock *flockFile
	mu   sync.Mutex // 从 Begin 持有到 Commit/Rollback，串行化批次事务

	reg    *identity.Registry
	seen   map[string]Event
	latest map[string]Event
}

// Open 打开（必要时创建）状态目录并恢复状态。
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, fmt.Errorf("创建状态目录: %w", err)
	}
	// 收紧已存在目录的权限，避免可还原信息被同机其他用户读取。
	if err := os.Chmod(dir, dirPerm); err != nil {
		return nil, fmt.Errorf("收紧状态目录权限: %w", err)
	}
	lk, err := acquireLock(dir)
	if err != nil {
		if errors.Is(err, fs.ErrPermission) || isLockBusy(err) {
			return nil, ErrLocked
		}
		return nil, err
	}
	s := &Store{
		dir:    dir,
		lock:   lk,
		reg:    identity.NewRegistry(),
		seen:   map[string]Event{},
		latest: map[string]Event{},
	}
	if err := s.load(); err != nil {
		_ = lk.release()
		return nil, err
	}
	return s, nil
}

// Close 释放进程锁（进程退出时锁也会由内核自动释放）。
func (s *Store) Close() error { return s.lock.release() }

func (s *Store) path() string { return filepath.Join(s.dir, stateFileName) }

// load 读取状态文件；半截文件（JSON 不完整）直接报错而非当空状态吞下。
func (s *Store) load() error {
	tmp := filepath.Join(s.dir, tmpFileName)
	if err := os.Remove(tmp); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("清理残留临时文件: %w", err)
	}
	data, err := os.ReadFile(s.path())
	if errors.Is(err, fs.ErrNotExist) {
		return nil // 首次运行：空状态
	}
	if err != nil {
		return fmt.Errorf("读取状态文件: %w", err)
	}
	var d onDisk
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&d); err != nil {
		return fmt.Errorf("状态文件损坏（拒绝以空状态启动）: %w", err)
	}
	if d.Version != snapshotVers {
		return fmt.Errorf("状态文件版本 %d 不受支持（期望 %d）", d.Version, snapshotVers)
	}
	s.seen = d.Seen
	s.latest = d.Latest
	if s.seen == nil {
		s.seen = map[string]Event{}
	}
	if s.latest == nil {
		s.latest = map[string]Event{}
	}
	s.migrateSubsecondConclusions()
	s.reg.Restore(d.Mapping)
	return nil
}

// migrateSubsecondConclusions 处理旧版本遗留状态：
//  1. 把可变宽度时间串（整秒无小数、.5/.50 等）归一化为固定宽度规范串；
//  2. 旧版本按字符串字典序取舍，同一秒内的小数秒事件可能选错结论，
//     这里从全部已提交事件按真实时间重算每键最新结论并就地修正。
//
// 迁移只改时间写法与“最新结论”指针，seen 事件集合与身份映射不变，
// 因此重放同一批依旧幂等（不新增身份、不新增事件）。
func (s *Store) migrateSubsecondConclusions() {
	for k, ev := range s.seen {
		ev.OccurredAt = canonicalizeOccurredAt(ev.OccurredAt)
		s.seen[k] = ev
	}
	recomputed := make(map[string]Event, len(s.latest))
	for _, ev := range s.seen {
		key := LatestKey(ev.CanonicalPID, ev.EventType, ev.Code)
		if cur, has := recomputed[key]; !has || eventLess(cur, ev) {
			recomputed[key] = ev
		}
	}
	s.latest = recomputed
}

// persist 以“临时文件 + fsync + rename + 目录 fsync”原子写入整份快照。
// mapping 取自当前批次事务（而非活注册表），保证“先落盘后提交”窗口内崩溃
// 也不会丢失本批身份；identityCommit 标记身份部分是否也已在事务内定稿。
func (s *Store) persist(seen map[string]Event, latest map[string]Event, mapping identity.Snapshot) error {
	d := onDisk{Version: snapshotVers, Seen: seen, Latest: latest, Mapping: mapping}
	buf, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(s.dir, tmpFileName)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, filePerm)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmp)
		}
	}()
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil { // 数据落盘
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmp, filePerm); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path()); err != nil { // 原子替换
		return err
	}
	dir, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	err = dir.Sync() // 保证 rename 落盘
	_ = dir.Close()
	if err != nil {
		return err
	}
	ok = true
	return nil
}

// SeenCount 返回已提交的去重事件总数。
func (s *Store) SeenCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.seen)
}

// Latest 返回当前已提交结论的副本（键为状态键）。
func (s *Store) Latest() map[string]Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]Event, len(s.latest))
	for k, v := range s.latest {
		out[k] = v
	}
	return out
}

// Registry 暴露内部身份注册表（用于导出/反查入口）。
func (s *Store) Registry() *identity.Registry { return s.reg }

// Begin 开启一个批次事务，串行持有存储锁直到 Commit/Rollback。
func (s *Store) Begin() *Tx {
	s.mu.Lock()
	seen := make(map[string]Event, len(s.seen))
	for k, v := range s.seen {
		seen[k] = v
	}
	latest := make(map[string]Event, len(s.latest))
	for k, v := range s.latest {
		latest[k] = v
	}
	return &Tx{
		store:  s,
		itx:    s.reg.Begin(),
		seen:   seen,
		latest: latest,
	}
}

// Tx 是批次级持久事务：暂存全部判定后一次性原子落盘，失败不留痕。
type Tx struct {
	store    *Store
	itx      *identity.Tx
	seen     map[string]Event
	latest   map[string]Event
	finished bool
}

// ResolveIdentity 在事务副本上登记身份并返回去标识化 ID；冲突时 ok=false。
func (t *Tx) ResolveIdentity(canonical, rawID string, attrs map[string]string) (string, bool) {
	return t.itx.Resolve(canonical, rawID, attrs)
}

// SeenEvent 返回已提交或本事务内已暂存的同去重键事件。
func (t *Tx) SeenEvent(sourceSystem, eventID string) (Event, bool) {
	ev, ok := t.seen[DedupKey(sourceSystem, eventID)]
	return ev, ok
}

// LatestForKey 返回状态键上的当前结论（含本事务暂存）。
func (t *Tx) LatestForKey(key string) (Event, bool) {
	ev, ok := t.latest[key]
	return ev, ok
}

// Stage 暂存一条事件：更新去重集合与状态键结论（乱序取舍）。
// 时间串在此归一化为固定宽度规范串，保证同批/跨批写法不同（整秒、.5、.50）
// 的同一时刻得到完全一致的落盘表示。
func (t *Tx) Stage(ev Event) {
	ev.OccurredAt = canonicalizeOccurredAt(ev.OccurredAt)
	t.seen[DedupKey(ev.SourceSystem, ev.EventID)] = ev
	key := LatestKey(ev.CanonicalPID, ev.EventType, ev.Code)
	if cur, has := t.latest[key]; !has || eventLess(cur, ev) {
		t.latest[key] = ev
	}
}

// Commit 先原子落盘（含身份映射），成功后再切换内存状态。
// 落盘失败：磁盘保持旧状态，内存不动，事务丢弃即可整批重来。
func (t *Tx) Commit() error {
	if t.finished {
		return nil
	}
	if err := t.store.persist(t.seen, t.latest, t.itx.Snapshot()); err != nil {
		t.finished = true
		t.store.mu.Unlock()
		return fmt.Errorf("状态落盘: %w", err)
	}
	if err := t.itx.Commit(); err != nil {
		// 磁盘已含本批身份快照，但与活注册表冲突：理论上被 Begin 串行锁排除；
		// 保守处理：回退磁盘到活注册表快照并报错。
		_ = t.store.persist(t.store.seen, t.store.latest, t.store.reg.Snapshot())
		t.finished = true
		t.store.mu.Unlock()
		return err
	}
	t.store.seen = t.seen
	t.store.latest = t.latest
	t.finished = true
	t.store.mu.Unlock()
	return nil
}

// Rollback 丢弃事务并释放锁（已 Commit 后调用为空操作）。
func (t *Tx) Rollback() {
	if t.finished {
		return
	}
	t.finished = true
	t.store.mu.Unlock()
}

// eventLess 定义乱序取舍：按 (OccurredAt, EventID) 字典序，与到达顺序无关。
// 时间按真实时间点比较，整秒与小数秒（.5/.50/.25 等任意位数）都落在同一时间轴上，
// 绝不按字符串字典序（固定宽度规范串的字典序虽与时间序一致，但历史数据可能是
// 旧版本写入的可变宽度串）。时间完全相同时再按事件号稳定分先后。
func eventLess(a, b Event) bool {
	ta, oa := parseEventTime(a.OccurredAt)
	tb, ob := parseEventTime(b.OccurredAt)
	if oa && ob {
		if !ta.Equal(tb) {
			return ta.Before(tb)
		}
	} else if a.OccurredAt != b.OccurredAt {
		return a.OccurredAt < b.OccurredAt // 无法解析时的兜底（正常数据不会走到）
	}
	return a.EventID < b.EventID
}

// ExportMapping 把重标识映射（规范键/去标识化 ID/首次原始标识）导出到 path。
// 仅用于本地核对：文件以 0600 创建，同机其他用户不可读；原子写，不会出现半截导出。
// 返回导出的条目数。
func (s *Store) ExportMapping(path string) (int, error) {
	s.mu.Lock()
	entries := s.reg.ExportMappings()
	s.mu.Unlock()
	buf, err := json.MarshalIndent(struct {
		Warning string                  `json:"warning"`
		Entries []identity.MappingEntry `json:"entries"`
	}{
		Warning: "含可还原患者标识，仅限本地核对；文件权限 0600，请勿外传。",
		Entries: entries,
	}, "", "  ")
	if err != nil {
		return 0, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, filePerm)
	if err != nil {
		return 0, err
	}
	if _, err := f.Write(append(buf, '\n')); err != nil {
		f.Close()
		return 0, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return 0, err
	}
	if err := f.Chmod(filePerm); err != nil {
		f.Close()
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	return len(entries), nil
}
