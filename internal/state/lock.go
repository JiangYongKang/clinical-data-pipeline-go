//go:build !windows

package state

import (
	"errors"
	"os"
	"syscall"
)

// isLockBusy 判断 flock 失败是否因为锁已被占用。
func isLockBusy(err error) bool {
	return errors.Is(err, syscall.EWOULDBLOCK)
}

// flockFile 持有状态目录上的排他进程锁（flock 随进程退出自动释放）。
type flockFile struct {
	f *os.File
}

func acquireLock(dir string) (*flockFile, error) {
	f, err := os.OpenFile(dir+string(os.PathSeparator)+lockFileName,
		os.O_CREATE|os.O_RDWR, filePerm)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, err
	}
	return &flockFile{f: f}, nil
}

func (l *flockFile) release() error {
	if l == nil || l.f == nil {
		return nil
	}
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	return l.f.Close()
}
