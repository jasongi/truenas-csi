package driver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
)

func TestRunISCSIAdmRunsOneSequenceAtATime(t *testing.T) {
	var running, peak atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := runISCSIAdm(context.Background(), logr.Discard(), "test", func() error {
				n := running.Add(1)
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
				time.Sleep(2 * time.Millisecond)
				running.Add(-1)
				return nil
			})
			if err != nil {
				t.Errorf("runISCSIAdm() = %v, want nil", err)
			}
		}()
	}
	wg.Wait()

	if got := peak.Load(); got != 1 {
		t.Errorf("peak concurrent iscsiadm sequences = %d, want 1", got)
	}
}

func TestRunISCSIAdmReleasesTheQueueAfterAnError(t *testing.T) {
	wantErr := errors.New("login failed")
	if err := runISCSIAdm(context.Background(), logr.Discard(), "test", func() error { return wantErr }); !errors.Is(err, wantErr) {
		t.Fatalf("runISCSIAdm() = %v, want %v", err, wantErr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := runISCSIAdm(ctx, logr.Discard(), "test", func() error { return nil }); err != nil {
		t.Fatalf("a call after a failed one should run, got %v", err)
	}
}

func TestRunISCSIAdmStopsWaitingWhenTheCallerGivesUp(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = runISCSIAdm(context.Background(), logr.Discard(), "holder", func() error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	ran := false
	err := runISCSIAdm(ctx, logr.Discard(), "waiter", func() error { ran = true; return nil })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("runISCSIAdm() = %v, want a deadline error", err)
	}
	if ran {
		t.Error("a caller whose context ended while queued must not run its sequence")
	}

	close(release)
	<-done
}

// withLockFixture points the sweep at a temporary lock directory and replaces the age
// lookup, because the inode change time of a real file cannot be set backwards.
func withLockFixture(t *testing.T, age time.Duration) (lock, lockWrite string) {
	t.Helper()
	dir := t.TempDir()
	lock = filepath.Join(dir, "lock")
	lockWrite = filepath.Join(dir, "lock.write")
	if err := os.WriteFile(lock, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	origPath, origAge := iscsiDBLockPath, iscsiDBLockAge
	iscsiDBLockPath = lockWrite
	iscsiDBLockAge = func(path string) (time.Duration, error) {
		if _, err := os.Stat(path); err != nil {
			return 0, err
		}
		return age, nil
	}
	t.Cleanup(func() { iscsiDBLockPath, iscsiDBLockAge = origPath, origAge })
	return lock, lockWrite
}

func TestRemoveStaleISCSIDBLock(t *testing.T) {
	tests := []struct {
		name        string
		age         time.Duration
		create      bool
		wantRemoved bool
	}{
		{name: "old lock is removed", age: 5 * time.Minute, create: true, wantRemoved: true},
		{name: "lock just past the threshold is removed", age: staleISCSIDBLockAge, create: true, wantRemoved: true},
		{name: "fresh lock of a live holder is kept", age: time.Second, create: true, wantRemoved: false},
		{name: "no lock is a no-op", age: 5 * time.Minute, create: false, wantRemoved: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lock, lockWrite := withLockFixture(t, tt.age)
			if tt.create {
				if err := os.Link(lock, lockWrite); err != nil {
					t.Fatal(err)
				}
			}

			removeStaleISCSIDBLock(logr.Discard())

			_, err := os.Stat(lockWrite)
			if removed := errors.Is(err, os.ErrNotExist); tt.create && removed != tt.wantRemoved {
				t.Errorf("lock.write removed = %v, want %v", removed, tt.wantRemoved)
			}
			if _, err := os.Stat(lock); err != nil {
				t.Errorf("the shared lock file must survive the sweep: %v", err)
			}
		})
	}
}

func TestRunISCSIAdmSweepsAStaleLockBeforeTheSequence(t *testing.T) {
	lock, lockWrite := withLockFixture(t, 5*time.Minute)
	if err := os.Link(lock, lockWrite); err != nil {
		t.Fatal(err)
	}

	var existedDuringRun bool
	err := runISCSIAdm(context.Background(), logr.Discard(), "test", func() error {
		_, statErr := os.Stat(lockWrite)
		existedDuringRun = statErr == nil
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if existedDuringRun {
		t.Error("the stale lock must be gone by the time the iscsiadm sequence starts")
	}
}

// A lock created just now must be judged young by the real age lookup, so a live
// holder is never swept.
func TestISCSIDBLockAgeOnDiskOfAFreshLink(t *testing.T) {
	dir := t.TempDir()
	lock := filepath.Join(dir, "lock")
	lockWrite := filepath.Join(dir, "lock.write")
	if err := os.WriteFile(lock, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(lock, lockWrite); err != nil {
		t.Fatal(err)
	}

	age, err := iscsiDBLockAgeOnDisk(lockWrite)
	if err != nil {
		t.Fatal(err)
	}
	if age < 0 || age >= staleISCSIDBLockAge {
		t.Errorf("age of a link made just now = %s, want under %s", age, staleISCSIDBLockAge)
	}
	if _, err := iscsiDBLockAgeOnDisk(filepath.Join(dir, "missing")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("age of a missing lock = %v, want a not-exist error", err)
	}
}
