package raft_test

import (
	"context"
	"testing"
	"time"

	"github.com/rakman09/jamraft/internal/jukebox"
)

// TestExactlyOnceEnqueue verifies that replaying the same (clientId, seq) command
// through the replicated log applies it exactly once.
func TestExactlyOnceEnqueue(t *testing.T) {
	c := newCluster(t, 3)
	if _, ok := c.WaitForLeader(3 * time.Second); !ok {
		t.Fatal("no leader")
	}

	cmd := jukebox.Command{
		Op:       jukebox.OpEnqueue,
		Song:     &jukebox.Song{ID: "x", Title: "duplicate-me"},
		ClientID: "clientA",
		Seq:      7,
	}.Encode()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Submit the identical command twice (as a retry would).
	if _, err := c.Submit(ctx, cmd); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	if _, err := c.Submit(ctx, cmd); err != nil {
		t.Fatalf("retry submit: %v", err)
	}

	// Despite two log entries, the queue must contain exactly one song.
	leader, _ := c.Leader()
	q := c.StateMachine(leader.ID()).View().Queue
	if len(q) != 1 {
		t.Fatalf("expected exactly-once (queue len 1), got %d: %+v", len(q), q)
	}
}
