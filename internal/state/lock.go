package state

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// lockName 是状态目录内的排他锁文件名。
const lockName = ".lock"

// Lock 获取状态目录的排他文件锁（非阻塞）。
// 另一个管线进程已持有锁时返回 ErrLocked，避免跨进程并发写坏状态。
// 锁在 Unlock（或进程退出）后自动释放。
func (m *Manager) Lock() error {
	if err := m.ensureDir(); err != nil {
		return err
	}
	p := filepath.Join(m.dir, lockName)
	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("打开状态锁文件失败: %w", err)
	}
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return fmt.Errorf("收紧锁文件权限失败: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return ErrLocked
		}
		return fmt.Errorf("获取状态排他锁失败: %w", err)
	}
	m.lockFile = f
	// 持锁后清理上次崩溃在 rename 前残留的临时文件。
	if err := m.cleanupTempFiles(); err != nil {
		_ = m.Unlock()
		return err
	}
	return nil
}

// Unlock 释放排他锁。
func (m *Manager) Unlock() error {
	if m.lockFile == nil {
		return nil
	}
	err1 := syscall.Flock(int(m.lockFile.Fd()), syscall.LOCK_UN)
	err2 := m.lockFile.Close()
	m.lockFile = nil
	if err1 != nil {
		return err1
	}
	return err2
}
