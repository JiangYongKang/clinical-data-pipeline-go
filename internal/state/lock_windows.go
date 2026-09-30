//go:build windows

package state

import "os"

// Windows 平台无 flock 等价物，锁退化为空操作（本地管线的目标平台是 Unix）。
type flockFile struct{}

func acquireLock(dir string) (*flockFile, error) { return &flockFile{}, nil }
func (l *flockFile) release() error              { return nil }

var _ = os.PathSeparator

func isLockBusy(err error) bool { return false }
