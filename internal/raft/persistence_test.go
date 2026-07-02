package raft_test

import (
	"testing"
	"time"

	"github.com/rakman09/jamraft/internal/cluster"
)

// waitQueueLen waits until the leader's state machine queue reaches n entries.
func waitQueueLen(t *testing.T, c *cluster.SimCluster, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leader, ok := c.Leader()
		if ok {
			if len(c.StateMachine(leader.ID()).View().Queue) == n {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("queue never reached length %d", n)
}

func TestSingleNodeRestartReplaysLog(t *testing.T) {
	c := cluster.New([]string{"solo"}, cluster.DefaultOptions())
	t.Cleanup(c.StopAll)
	if _, ok := c.WaitForLeader(2 * time.Second); !ok {
		t.Fatal("no leader")
	}
	submitEnqueue(t, c, "s1")
	submitEnqueue(t, c, "s2")

	c.Crash("solo")
	c.Restart("solo")
	if _, ok := c.WaitForLeader(2 * time.Second); !ok {
		t.Fatal("no leader after restart")
	}
	waitQueueLen(t, c, 2, 3*time.Second)
}

func TestFullClusterRecovery(t *testing.T) {
	ids := []string{"A", "B", "C"}
	c := cluster.New(ids, cluster.DefaultOptions())
	t.Cleanup(c.StopAll)
	if _, ok := c.WaitForLeader(3 * time.Second); !ok {
		t.Fatal("no leader")
	}
	for _, s := range []string{"s1", "s2", "s3", "s4"} {
		submitEnqueue(t, c, s)
	}
	waitQueueLen(t, c, 4, 3*time.Second)

	// Kill every node, then restart them all: the committed queue must survive.
	for _, id := range ids {
		c.Crash(id)
	}
	for _, id := range ids {
		c.Restart(id)
	}
	if _, ok := c.WaitForLeader(4 * time.Second); !ok {
		t.Fatal("no leader after full restart")
	}
	waitQueueLen(t, c, 4, 5*time.Second)
}
