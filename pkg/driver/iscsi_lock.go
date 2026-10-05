package driver

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"

	"github.com/go-logr/logr"
)

// Why iscsiadm calls are serialised, and why a stale lock file is swept.
//
// open-iscsi guards its node database with a hard-link lock: it creates
// <lock dir>/lock.write as a link to <lock dir>/lock, and a second process that finds
// the link already there polls every 10 ms for 30 s before it gives up (exit 6,
// "Timeout on acquiring lock on DB") and unlinks the file itself.
//
// csi-lib-iscsi kills every iscsiadm it starts after 3 s. Calls that need the database
// lock (discovery, login, logout, record deletion) run one at a time at roughly 0.4 s
// each, so a burst of concurrent NodeStageVolume or NodeUnstageVolume calls (a node
// drain, a rollout, many pods restarting) queues them past 3 s. The call that holds the
// lock is then killed mid-operation and lock.write is left behind. Every later call
// blocks on the stale file and is killed at 3 s, before its own 30 s timeout could
// unlink it, so the node cannot stage or unstage any iSCSI volume until its tmpfs is
// cleared by a reboot.
//
// Two measures remove this: iscsiadm sequences run one at a time per node, so the queue
// never builds inside open-iscsi, and a lock.write that is older than any real holder is
// removed before the next sequence starts.

const (
	// defaultISCSIDBLockPath is open-iscsi's write lock as seen from the node plugin
	// container, which reaches the host's /run through /host/proc/1/root (the same route
	// the iscsiadm wrapper uses to enter the host's mount namespace).
	defaultISCSIDBLockPath = "/host/proc/1/root/run/lock/iscsi/lock.write"

	// iscsiDBLockPathEnv overrides the lock path for hosts that lay the node out
	// differently.
	iscsiDBLockPathEnv = "TRUENAS_ISCSI_DB_LOCK_PATH"

	// staleISCSIDBLockAge is how old lock.write must be before it is treated as left
	// behind. A real holder keeps it for well under a second and csi-lib-iscsi kills
	// its own calls after 3 s, so a lock this old cannot belong to a live call of ours.
	staleISCSIDBLockAge = 20 * time.Second

	// iscsiAdmQueueWait bounds how long a call waits for its turn.
	iscsiAdmQueueWait = 2 * time.Minute
)

// iscsiDBLockPath is the lock file to sweep. Overridable in tests.
var iscsiDBLockPath = lockPathFromEnv()

// iscsiDBLockAge reports how long ago lock.write was created. Overridable in tests.
var iscsiDBLockAge = iscsiDBLockAgeOnDisk

// iscsiAdmSem admits one iscsiadm sequence at a time on this node.
var iscsiAdmSem = make(chan struct{}, 1)

func lockPathFromEnv() string {
	if p := os.Getenv(iscsiDBLockPathEnv); p != "" {
		return p
	}
	return defaultISCSIDBLockPath
}

// runISCSIAdm runs fn, which makes iscsiadm calls, as the only such sequence on this
// node and after removing a stale database lock. It gives up if ctx ends or the queue
// does not clear within iscsiAdmQueueWait.
func runISCSIAdm(ctx context.Context, log logr.Logger, op string, fn func() error) error {
	waitCtx, cancel := context.WithTimeout(ctx, iscsiAdmQueueWait)
	defer cancel()

	queued := time.Now()
	select {
	case iscsiAdmSem <- struct{}{}:
	case <-waitCtx.Done():
		return fmt.Errorf("waited %s for earlier iscsiadm work to finish before %s: %w",
			time.Since(queued).Round(time.Millisecond), op, waitCtx.Err())
	}
	defer func() { <-iscsiAdmSem }()

	if waited := time.Since(queued); waited >= time.Second {
		log.V(LogLevelDebug).Info("Waited for earlier iscsiadm work", "operation", op, "waited", waited.Round(time.Millisecond).String())
	}

	removeStaleISCSIDBLock(log)
	return fn()
}

// removeStaleISCSIDBLock deletes open-iscsi's lock.write when it is old enough that no
// live call can be holding it. Failures are logged and otherwise ignored: the sweep is
// a repair and must never make a stage or unstage fail by itself.
func removeStaleISCSIDBLock(log logr.Logger) {
	path := iscsiDBLockPath
	age, err := iscsiDBLockAge(path)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			log.V(LogLevelDebug).Info("Could not inspect the open-iscsi database lock", "path", path, "error", err.Error())
		}
		return
	}
	if age < staleISCSIDBLockAge {
		return
	}
	if err := os.Remove(path); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			log.Error(err, "Could not remove a stale open-iscsi database lock", "path", path)
		}
		return
	}
	log.Info("Removed a stale open-iscsi database lock left by a killed iscsiadm", "path", path, "age", age.Round(time.Second).String())
}
