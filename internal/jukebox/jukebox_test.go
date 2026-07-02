package jukebox

import (
	"encoding/json"
	"testing"
)

func apply(t *testing.T, j *Jukebox, c Command) Result {
	t.Helper()
	out := j.Apply(c.Encode())
	var r Result
	if err := json.Unmarshal(out, &r); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	return r
}

func TestEnqueuePlayNext(t *testing.T) {
	j := New()
	apply(t, j, Command{Op: OpEnqueue, Song: &Song{ID: "1", Title: "A"}})
	apply(t, j, Command{Op: OpEnqueue, Song: &Song{ID: "2", Title: "B"}})

	r := apply(t, j, Command{Op: OpPlayNext})
	if r.NowPlaying == nil || r.NowPlaying.Title != "A" {
		t.Fatalf("expected A playing, got %+v", r.NowPlaying)
	}
	if len(r.Queue) != 1 || r.Queue[0].Title != "B" {
		t.Fatalf("expected [B] in queue, got %+v", r.Queue)
	}

	r = apply(t, j, Command{Op: OpPlayNext})
	if r.NowPlaying == nil || r.NowPlaying.Title != "B" {
		t.Fatalf("expected B playing, got %+v", r.NowPlaying)
	}
}

func TestDedup(t *testing.T) {
	j := New()
	c := Command{Op: OpEnqueue, Song: &Song{ID: "1", Title: "A"}, ClientID: "c1", Seq: 1}
	apply(t, j, c)
	apply(t, j, c) // retry with same seq must not double-apply
	r := apply(t, j, Command{Op: OpPlayNext})
	if len(r.Queue) != 0 {
		t.Fatalf("expected queue drained (no duplicate), got %+v", r.Queue)
	}
	if r.NowPlaying == nil || r.NowPlaying.Title != "A" {
		t.Fatalf("expected A playing, got %+v", r.NowPlaying)
	}
}

func TestVoteSkipThreshold(t *testing.T) {
	j := New()
	j.st.SkipThreshold = 2
	apply(t, j, Command{Op: OpEnqueue, Song: &Song{ID: "1", Title: "A"}})
	apply(t, j, Command{Op: OpEnqueue, Song: &Song{ID: "2", Title: "B"}})
	apply(t, j, Command{Op: OpPlayNext}) // A playing
	apply(t, j, Command{Op: OpVoteSkip, Voter: "u1"})
	r := apply(t, j, Command{Op: OpVoteSkip, Voter: "u2"})
	if r.NowPlaying == nil || r.NowPlaying.Title != "B" {
		t.Fatalf("expected auto-skip to B, got %+v", r.NowPlaying)
	}
}

func TestReorder(t *testing.T) {
	j := New()
	apply(t, j, Command{Op: OpEnqueue, Song: &Song{ID: "1", Title: "A"}})
	apply(t, j, Command{Op: OpEnqueue, Song: &Song{ID: "2", Title: "B"}})
	apply(t, j, Command{Op: OpEnqueue, Song: &Song{ID: "3", Title: "C"}})
	r := apply(t, j, Command{Op: OpReorder, SongID: "3", ToIndex: 0})
	if r.Queue[0].Title != "C" || r.Queue[1].Title != "A" || r.Queue[2].Title != "B" {
		t.Fatalf("unexpected order: %+v", r.Queue)
	}
}

func TestSnapshotRestore(t *testing.T) {
	j := New()
	apply(t, j, Command{Op: OpEnqueue, Song: &Song{ID: "1", Title: "A"}})
	apply(t, j, Command{Op: OpEnqueue, Song: &Song{ID: "2", Title: "B"}})
	apply(t, j, Command{Op: OpPlayNext})
	snap := j.Snapshot()

	j2 := New()
	if err := j2.Restore(snap); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if j2.View().NowPlaying.Title != "A" {
		t.Fatalf("restore lost now-playing")
	}
	if len(j2.View().Queue) != 1 || j2.View().Queue[0].Title != "B" {
		t.Fatalf("restore lost queue")
	}
}
