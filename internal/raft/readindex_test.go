package raft_test

import (
	"context"
	"testing"
	"time"

	"github.com/rakman09/jamraft/internal/raft"
)

func TestLinearizableReadReflectsWrites(t *testing.T) {
	c := newCluster(t, 3)
	leader, ok := c.WaitForLeader(3 * time.Second)
	if !ok {
		t.Fatal("no leader")
	}
	submitEnqueue(t, c, "a")
	submitEnqueue(t, c, "b")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	idx, err := leader.LinearizableRead(ctx)
	if err != nil {
		t.Fatalf("read-index: %v", err)
	}
	if idx == 0 {
		t.Fatal("read index should be > 0")
	}
	q := c.StateMachine(leader.ID()).View().Queue
	if len(q) != 2 {
		t.Fatalf("expected 2 songs visible after read-index, got %d", len(q))
	}
}

func TestIsolatedLeaderCannotServeStaleRead(t *testing.T) {
	c := newCluster(t, 5)
	leader, ok := c.WaitForLeader(3 * time.Second)
	if !ok {
		t.Fatal("no leader")
	}
	submitEnqueue(t, c, "a")

	// Partition the leader into a minority (itself alone).
	c.Net.Isolate(leader.ID())

	// The isolated leader must FAIL a linearizable read (cannot confirm
	// leadership with a majority), rather than return stale data.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if _, err := leader.LinearizableRead(ctx); err == nil {
		t.Fatal("isolated leader served a linearizable read; expected failure")
	}

	// The majority partition should elect a new leader that CAN serve reads.
	deadline := time.Now().Add(4 * time.Second)
	var newLeader *raft.Node
	for time.Now().Before(deadline) {
		for _, id := range c.IDs() {
			if id == leader.ID() {
				continue
			}
			n := c.Node(id)
			if n != nil && n.IsLeader() {
				newLeader = n
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatal("majority did not elect a new leader")
	}
	rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer rcancel()
	if _, err := newLeader.LinearizableRead(rctx); err != nil {
		t.Fatalf("new leader could not serve read: %v", err)
	}
}
