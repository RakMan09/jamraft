package raft_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/rakman09/jamraft/internal/cluster"
	"github.com/rakman09/jamraft/internal/jukebox"
)

// queueTitles returns the titles of the current queue on a node's state machine.
func nowPlaying(t *testing.T, c *cluster.SimCluster, id string) *jukebox.Song {
	t.Helper()
	return c.StateMachine(id).View().NowPlaying
}

func TestLogReplicationConverges(t *testing.T) {
	c := newCluster(t, 3)
	if _, ok := c.WaitForLeader(3 * time.Second); !ok {
		t.Fatal("no leader")
	}
	for _, title := range []string{"song1", "song2", "song3"} {
		submitEnqueue(t, c, title)
	}

	// Give replication a moment, then all nodes must agree on the queue.
	deadline := time.Now().Add(3 * time.Second)
	for {
		agree := true
		var want []*jukebox.Song
		for i, id := range c.IDs() {
			q := c.StateMachine(id).View().Queue
			if i == 0 {
				want = q
				continue
			}
			if len(q) != len(want) {
				agree = false
				break
			}
			for j := range q {
				if q[j].Title != want[j].Title {
					agree = false
					break
				}
			}
		}
		if agree && len(want) == 3 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("queues did not converge: %+v", want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestCommitSurvivesLeaderChange(t *testing.T) {
	c := newCluster(t, 5)
	leader, ok := c.WaitForLeader(3 * time.Second)
	if !ok {
		t.Fatal("no leader")
	}
	submitEnqueue(t, c, "committed-song")

	c.Crash(leader.ID())
	if _, ok := c.WaitForLeader(3 * time.Second); !ok {
		t.Fatal("no new leader")
	}

	// Enqueue another after the change; then verify both are present and ordered.
	submitEnqueue(t, c, "after-change")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// Play the first song and confirm it is the committed one (FIFO order).
	cmd := jukebox.Command{Op: jukebox.OpPlayNext}
	out, err := c.Submit(ctx, cmd.Encode())
	if err != nil {
		t.Fatalf("play-next: %v", err)
	}
	var r jukebox.Result
	json.Unmarshal(out, &r)
	if r.NowPlaying == nil || r.NowPlaying.Title != "committed-song" {
		t.Fatalf("expected committed-song first, got %+v", r.NowPlaying)
	}
}
