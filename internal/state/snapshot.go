package state

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/identity"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/pipeline"
)

// snapshotFormat 是落盘文件的封装格式。
type snapshotFormat struct {
	Version  int                    `json:"version"`
	Checksum string                 `json:"checksum"` // 对 Payload 规范化 JSON 的 SHA-256（hex）
	Identity identity.Snapshot      `json:"identity"`
	Store    pipeline.StoreSnapshot `json:"store"`
}

// payload 是被校验和覆盖的部分（Checksum 自身除外）。
func (f snapshotFormat) payload() ([]byte, error) {
	type p struct {
		Version  int                    `json:"version"`
		Identity identity.Snapshot      `json:"identity"`
		Store    pipeline.StoreSnapshot `json:"store"`
	}
	return json.MarshalIndent(p{Version: f.Version, Identity: f.Identity, Store: f.Store}, "", "  ")
}

func checksum(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Save 原子写入一份完整快照。
// 步骤：确保目录 0700 -> 写临时文件 0600 -> 校验和 -> fsync 文件 -> rename -> fsync 目录。
// 任意一步中断都不会出现半截状态文件：rename 之前崩溃只留下会被忽略/清理的临时文件。
func (m *Manager) Save(idSnap identity.Snapshot, storeSnap pipeline.StoreSnapshot) error {
	if err := m.ensureDir(); err != nil {
		return err
	}
	f := snapshotFormat{Version: 1, Identity: idSnap, Store: storeSnap}
	payloadBytes, err := f.payload()
	if err != nil {
		return fmt.Errorf("编码状态快照失败: %w", err)
	}
	f.Checksum = checksum(payloadBytes)
	body, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return fmt.Errorf("编码状态文件失败: %w", err)
	}

	final := filepath.Join(m.dir, fileName)
	tmp, err := os.CreateTemp(m.dir, ".pipeline-state.tmp-*")
	if err != nil {
		return fmt.Errorf("创建临时状态文件失败: %w", err)
	}
	tmpName := tmp.Name()
	// 临时文件权限收住为 0600（CreateTemp 通常已是 0600，这里显式保证）。
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("收紧临时文件权限失败: %w", err)
	}
	cleanup := func() {
		tmp.Close()
		os.Remove(tmpName)
	}
	if _, err := tmp.Write(append(body, '\n')); err != nil {
		cleanup()
		return fmt.Errorf("写临时状态文件失败: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("刷盘临时状态文件失败: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("关闭临时状态文件失败: %w", err)
	}
	if err := os.Rename(tmpName, final); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("原子替换状态文件失败: %w", err)
	}
	// fsync 目录，保证 rename 在掉电后也已持久化。
	if err := syncDir(m.dir); err != nil {
		return fmt.Errorf("刷盘状态目录失败: %w", err)
	}
	return nil
}

// Load 读回状态文件并校验完整性；文件不存在视为初始空状态。
func (m *Manager) Load() (identity.Snapshot, pipeline.StoreSnapshot, error) {
	final := filepath.Join(m.dir, fileName)
	body, err := os.ReadFile(final)
	if err != nil {
		if os.IsNotExist(err) {
			return identity.Snapshot{}, pipeline.StoreSnapshot{}, nil
		}
		return identity.Snapshot{}, pipeline.StoreSnapshot{}, fmt.Errorf("读状态文件失败: %w", err)
	}
	var f snapshotFormat
	if err := json.Unmarshal(body, &f); err != nil {
		return identity.Snapshot{}, pipeline.StoreSnapshot{}, fmt.Errorf("状态文件不是合法 JSON（可能写入中断或损坏）: %w", err)
	}
	if f.Version != 1 {
		return identity.Snapshot{}, pipeline.StoreSnapshot{}, fmt.Errorf("状态文件版本 %d 不被支持", f.Version)
	}
	payloadBytes, err := f.payload()
	if err != nil {
		return identity.Snapshot{}, pipeline.StoreSnapshot{}, fmt.Errorf("重算状态校验和失败: %w", err)
	}
	if checksum(payloadBytes) != f.Checksum {
		return identity.Snapshot{}, pipeline.StoreSnapshot{},
			fmt.Errorf("状态文件校验和不匹配（损坏或被篡改），拒绝载入")
	}
	return f.Identity, f.Store, nil
}

// ensureDir 以 0700 创建状态目录；已存在则把权限收紧到 0700。
func (m *Manager) ensureDir() error {
	if err := os.MkdirAll(m.dir, 0o700); err != nil {
		return fmt.Errorf("创建状态目录失败: %w", err)
	}
	if err := os.Chmod(m.dir, 0o700); err != nil {
		return fmt.Errorf("收紧状态目录权限失败: %w", err)
	}
	return nil
}

// cleanupTempFiles 清理上次写入在 rename 前崩溃留下的临时文件。
// 这些文件从未成为正式状态文件，删除不影响任何已提交状态。
// 必须在持有排他锁后调用，避免误删另一个进程正在写入的临时文件。
func (m *Manager) cleanupTempFiles() error {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return fmt.Errorf("读取状态目录失败: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if matched, _ := filepath.Match(".pipeline-state.tmp-*", e.Name()); matched {
			if err := os.Remove(filepath.Join(m.dir, e.Name())); err != nil {
				return fmt.Errorf("清理残留临时文件 %s 失败: %w", e.Name(), err)
			}
		}
	}
	return nil
}

// syncDir 打开目录并 fsync，使其中的 rename/create 落盘。
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
