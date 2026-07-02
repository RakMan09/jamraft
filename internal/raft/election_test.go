package raft_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/rakman09/jamraft/internal/cluster"
	"github.com/rakman09/jamraft/internal/jukebox"
)

func newCluster(t *testing.T, n int) *cluster.SimCluster {
	t.Helper()
	ids := make([]string, n)
	for i := range ids {
		ids[i] = string(rune('A' + i))
	}
	c := cluster.New(ids, cluster.DefaultOptions())
	t.Cleanup(c.StopAll)
	return c
}

func TestLeaderElection(t *testing.T) {
	c := newCluster(t, 3)
	leader, ok := c.WaitForLeader(3 * time.Second)
	if !ok {
		t.Fatal("no leader elected")
	}
	t.Logf("leader elected: %s", leader.ID())

	// Exactly one leader.
	if _, ok := c.Leader(); !ok {
		t.Fatal("expected exactly one leader")
	}
}

func TestReElectionAfterLeaderCrash(t *testing.T) {
	c := newCluster(t, 3)
	leader, ok := c.WaitForLeader(3 * time.Second)
	if !ok {
		t.Fatal("no leader elected")
	}
	oldLeader := leader.ID()
	c.Crash(oldLeader)

	// A new leader should emerge among the remaining two.
	newLeader, ok := c.WaitForLeader(3 * time.Second)
	if !ok {
		t.Fatal("no new leader after crash")
	}
	if newLeader.ID() == oldLeader {
		t.Fatalf("crashed leader still leader")
	}
	t.Logf("new leader after crash: %s", newLeader.ID())
}

func TestSingleNodeElection(t *testing.T) {
	c := cluster.New([]string{"solo"}, cluster.DefaultOptions())
	t.Cleanup(c.StopAll)
	if _, ok := c.WaitForLeader(2 * time.Second); !ok {
		t.Fatal("single node did not become leader")
	}
}

// submit is a small helper used by replication/persistence tests.
func submitEnqueue(t *testing.T, c *cluster.SimCluster, title string) jukebox.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := jukebox.Command{Op: jukebox.OpEnqueue, Song: &jukebox.Song{ID: title, Title: title}}
	out, err := c.Submit(ctx, cmd.Encode())
	if err != nil {
		t.Fatalf("submit %s: %v", title, err)
	}
	var r jukebox.Result
	if err := json.Unmarshal(out, &r); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return r
}
