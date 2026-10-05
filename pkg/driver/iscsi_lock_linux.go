//go:build linux

package driver

import (
	"syscall"
	"time"
)

// iscsiDBLockAgeOnDisk returns the time since the lock file was created. lock.write is
// a hard link to the shared lock inode, so its modification time is that of the older
// lock file and says nothing about when the link was made; the inode change time moves
// when the link is created, which is what matters here.
func iscsiDBLockAgeOnDisk(path string) (time.Duration, error) {
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		return 0, err
	}
	sec, nsec := st.Ctim.Unix()
	return time.Since(time.Unix(sec, nsec)), nil
}
