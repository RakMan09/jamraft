package raft_test

import (
	"testing"
	"time"

	"github.com/rakman09/jamraft/internal/cluster"
)

func TestSnapshotCompactsLog(t *testing.T) {
	opts := cluster.DefaultOptions()
	opts.SnapshotThreshold = 5
	c := cluster.New([]string{"A", "B", "C"}, opts)
	t.Cleanup(c.StopAll)

	leader, ok := c.WaitForLeader(3 * time.Second)
	if !ok {
		t.Fatal("no leader")
	}
	for i := 0; i < 30; i++ {
		submitEnqueue(t, c, "song")
	}

	// The leader must have compacted its log via a snapshot.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		st := leader.Status()
		if st.SnapshotIdx > 0 && st.LogSize <= opts.SnapshotThreshold+5 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("log was not compacted: %+v", leader.Status())
}

func TestInstallSnapshotCatchesUpLaggingFollower(t *testing.T) {
	opts := cluster.DefaultOptions()
	opts.SnapshotThreshold = 5
	ids := []string{"A", "B", "C"}
	c := cluster.New(ids, opts)
	t.Cleanup(c.StopAll)

	leader, ok := c.WaitForLeader(3 * time.Second)
	if !ok {
		t.Fatal("no leader")
	}

	// Pick a follower to take offline.
	var follower string
	for _, id := range ids {
		if id != leader.ID() {
			follower = id
			break
		}
	}
	c.Crash(follower)

	// Enqueue enough to force the leader to snapshot well past the follower.
	for i := 0; i < 40; i++ {
		submitEnqueue(t, c, "song")
	}
	waitQueueLen(t, c, 40, 4*time.Second)

	// Restart the lagging follower; it must catch up (via InstallSnapshot).
	c.Restart(follower)

	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if len(c.StateMachine(follower).View().Queue) == 40 {
			st := c.Node(follower).Status()
			if st.SnapshotIdx > 0 {
				return // caught up and received a snapshot
			}
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatalf("follower did not catch up via snapshot: %+v", c.Node(follower).Status())
}
