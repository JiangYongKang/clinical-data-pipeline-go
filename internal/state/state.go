// Package state 把管线的已提交结论与去标识化对应关系落到本地单个文件，
// 并保证写入的原子性、读回的完整性与文件权限的最小化。
//
// 持久化边界：
//   - 状态目录权限 0700、状态文件权限 0600，同机其他用户不可读。
//   - 写入采用“临时文件 + fsync + 原子 rename”，写到一半被中断不会留下半截文件，
//     下次启动要么读到旧完整版本，要么读到新完整版本。
//   - 文件内容含 SHA-256 校验和，损坏或被篡改时拒绝载入并返回错误，绝不读到坏数据。
//   - 通过 flock 排他锁阻止两个进程同时写同一状态目录。
package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/identity"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/pipeline"
)

// DefaultDir 是未显式指定状态目录时使用的本地目录。
const DefaultDir = ".pipeline-state"

// fileName 是状态目录内的单一状态文件名。
const fileName = "pipeline-state.json"

// ErrLocked 表示状态目录正被另一个管线进程独占。
var ErrLocked = errors.New("状态目录已被另一个管线进程锁定")

// Manager 管理一个本地状态目录内的持久化文件。
// 它实现 pipeline.Persister：每个成功批次原子替换一次完整快照。
type Manager struct {
	dir      string
	lockFile *os.File
}

// New 创建或打开状态目录（权限 0700）。调用方还需 Lock 后再读写。
func New(dir string) *Manager {
	return &Manager{dir: dir}
}

// Dir 返回状态目录路径。
func (m *Manager) Dir() string { return m.dir }

// Persist 实现 pipeline.Persister：原子替换本地状态文件。
func (m *Manager) Persist(idSnap identity.Snapshot, storeSnap pipeline.StoreSnapshot) error {
	return m.Save(idSnap, storeSnap)
}

// MappingEntry 是导出给人核对的一行对应关系。
type MappingEntry struct {
	DeidentifiedID string `json:"deidentifiedId"`
	CanonicalPID   string `json:"canonicalPid"`
	RawPatientID   string `json:"rawPatientId"`
}

// ExportMapping 把去标识化对应关系导出为本地核对文件（JSON 数组，0600）。
// 该文件含可还原信息，只应用于本地核对：必须显式指定导出路径，
// 且文件权限收住为 0600，不随普通结果输出产生。
func (m *Manager) ExportMapping(outPath string) (int, error) {
	idSnap, _, err := m.Load()
	if err != nil {
		return 0, err
	}
	entries := make([]MappingEntry, 0, len(idSnap.Records))
	for _, rec := range idSnap.Records {
		entries = append(entries, MappingEntry{
			DeidentifiedID: rec.Deidentified,
			CanonicalPID:   rec.Canonical,
			RawPatientID:   rec.RawExample,
		})
	}
	body, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return 0, fmt.Errorf("编码映射导出失败: %w", err)
	}
	f, err := os.OpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return 0, fmt.Errorf("创建导出文件失败: %w", err)
	}
	defer f.Close()
	if err := f.Chmod(0o600); err != nil {
		return 0, fmt.Errorf("收紧导出文件权限失败: %w", err)
	}
	if _, err := f.Write(append(body, '\n')); err != nil {
		return 0, fmt.Errorf("写导出文件失败: %w", err)
	}
	if err := f.Sync(); err != nil {
		return 0, fmt.Errorf("刷盘导出文件失败: %w", err)
	}
	return len(entries), nil
}
