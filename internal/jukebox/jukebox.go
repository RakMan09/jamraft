// Package jukebox implements the replicated state machine that JamRaft drives:
// a shared party music queue. Applying a committed log command mutates the
// queue deterministically, so every node that applies the same log ends up in
// the same state.
package jukebox

import (
	"encoding/json"
	"fmt"
)

// Op names for jukebox commands.
const (
	OpEnqueue  = "enqueue"
	OpPlayNext = "play-next"
	OpVoteSkip = "vote-skip"
	OpReorder  = "reorder"
	OpNoop     = "noop"
)

// Song is one item in the queue.
type Song struct {
	ID      string `json:"id"`
	Title   string `json:"title"`
	AddedBy string `json:"addedBy,omitempty"`
}

// Command is a single jukebox operation. It is JSON-encoded and stored as the
// opaque payload of a Raft log entry.
type Command struct {
	Op       string `json:"op"`
	ClientID string `json:"clientId,omitempty"`
	Seq      uint64 `json:"seq,omitempty"`

	// enqueue
	Song *Song `json:"song,omitempty"`

	// vote-skip / reorder
	SongID  string `json:"songId,omitempty"`
	ToIndex int    `json:"toIndex,omitempty"`
	Voter   string `json:"voter,omitempty"`
}

// Encode marshals a command to bytes for use as a log entry payload.
func (c Command) Encode() []byte {
	b, _ := json.Marshal(c)
	return b
}

// Result is returned from Apply and surfaced to the submitting client.
type Result struct {
	OK         bool    `json:"ok"`
	Error      string  `json:"error,omitempty"`
	NowPlaying *Song   `json:"nowPlaying,omitempty"`
	Queue      []*Song `json:"queue,omitempty"`
	SkipVotes  int     `json:"skipVotes,omitempty"`
}

// State is the full jukebox state. It is exported so it can be snapshotted and
// restored via JSON.
type State struct {
	Queue      []*Song `json:"queue"`
	NowPlaying *Song   `json:"nowPlaying"`
	History    []*Song `json:"history"`

	// SkipVotes holds voter clientIds for the currently playing song.
	SkipVotes map[string]bool `json:"skipVotes"`

	// SkipThreshold is the number of votes that auto-advances the song.
	SkipThreshold int `json:"skipThreshold"`

	// Client session dedup state: last applied seq and its result per client.
	// This lives inside the state machine so exactly-once semantics survive
	// leader changes and are identical on every replica.
	LastSeq    map[string]uint64 `json:"lastSeq"`
	LastResult map[string][]byte `json:"lastResult"`
}

// Jukebox is the state machine wrapper around State.
type Jukebox struct {
	st State
}

// New returns an empty jukebox.
func New() *Jukebox {
	return &Jukebox{st: State{
		SkipVotes:     map[string]bool{},
		SkipThreshold: 3,
		LastSeq:       map[string]uint64{},
		LastResult:    map[string][]byte{},
	}}
}

// Apply applies one command (JSON-encoded) and returns the JSON-encoded Result.
// It is deterministic: identical input on identical prior state yields identical
// output and next state.
func (j *Jukebox) Apply(cmd []byte) []byte {
	var c Command
	if err := json.Unmarshal(cmd, &c); err != nil {
		r, _ := json.Marshal(Result{OK: false, Error: fmt.Sprintf("bad command: %v", err)})
		return r
	}

	// Exactly-once: if we've already applied this (clientId, seq), replay the
	// cached result instead of re-applying the mutation.
	if c.ClientID != "" && c.Seq != 0 {
		if last, ok := j.st.LastSeq[c.ClientID]; ok && c.Seq <= last {
			if res, ok := j.st.LastResult[c.ClientID]; ok {
				return res
			}
		}
	}

	res := j.applyInner(c)
	out, _ := json.Marshal(res)

	if c.ClientID != "" && c.Seq != 0 {
		j.st.LastSeq[c.ClientID] = c.Seq
		j.st.LastResult[c.ClientID] = out
	}
	return out
}

func (j *Jukebox) applyInner(c Command) Result {
	switch c.Op {
	case OpNoop:
		return Result{OK: true}

	case OpEnqueue:
		if c.Song == nil || c.Song.Title == "" {
			return Result{OK: false, Error: "enqueue requires a song with a title"}
		}
		s := *c.Song
		j.st.Queue = append(j.st.Queue, &s)
		return j.snapshotResult(true, "")

	case OpPlayNext:
		if j.st.NowPlaying != nil {
			j.st.History = append(j.st.History, j.st.NowPlaying)
		}
		if len(j.st.Queue) == 0 {
			j.st.NowPlaying = nil
		} else {
			j.st.NowPlaying = j.st.Queue[0]
			j.st.Queue = j.st.Queue[1:]
		}
		j.st.SkipVotes = map[string]bool{}
		return j.snapshotResult(true, "")

	case OpVoteSkip:
		if j.st.NowPlaying == nil {
			return Result{OK: false, Error: "nothing playing"}
		}
		voter := c.Voter
		if voter == "" {
			voter = c.ClientID
		}
		if voter == "" {
			return Result{OK: false, Error: "vote-skip requires a voter"}
		}
		j.st.SkipVotes[voter] = true
		if len(j.st.SkipVotes) >= j.st.SkipThreshold {
			// Auto-advance.
			return j.applyInner(Command{Op: OpPlayNext})
		}
		return j.snapshotResult(true, "")

	case OpReorder:
		from := -1
		for i, s := range j.st.Queue {
			if s.ID == c.SongID {
				from = i
				break
			}
		}
		if from < 0 {
			return Result{OK: false, Error: "song not found in queue"}
		}
		to := c.ToIndex
		if to < 0 {
			to = 0
		}
		if to >= len(j.st.Queue) {
			to = len(j.st.Queue) - 1
		}
		s := j.st.Queue[from]
		j.st.Queue = append(j.st.Queue[:from], j.st.Queue[from+1:]...)
		// insert at to
		j.st.Queue = append(j.st.Queue, nil)
		copy(j.st.Queue[to+1:], j.st.Queue[to:])
		j.st.Queue[to] = s
		return j.snapshotResult(true, "")

	default:
		return Result{OK: false, Error: "unknown op: " + c.Op}
	}
}

func (j *Jukebox) snapshotResult(ok bool, errMsg string) Result {
	return Result{
		OK:         ok,
		Error:      errMsg,
		NowPlaying: j.st.NowPlaying,
		Queue:      j.cloneQueue(),
		SkipVotes:  len(j.st.SkipVotes),
	}
}

func (j *Jukebox) cloneQueue() []*Song {
	out := make([]*Song, len(j.st.Queue))
	copy(out, j.st.Queue)
	return out
}

// View returns a read-only snapshot of the current visible state (for reads).
func (j *Jukebox) View() Result {
	return j.snapshotResult(true, "")
}

// Snapshot serializes the entire state for log compaction.
func (j *Jukebox) Snapshot() []byte {
	b, _ := json.Marshal(j.st)
	return b
}

// Restore replaces the entire state from a snapshot produced by Snapshot.
func (j *Jukebox) Restore(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	var st State
	if err := json.Unmarshal(data, &st); err != nil {
		return err
	}
	if st.SkipVotes == nil {
		st.SkipVotes = map[string]bool{}
	}
	if st.LastSeq == nil {
		st.LastSeq = map[string]uint64{}
	}
	if st.LastResult == nil {
		st.LastResult = map[string][]byte{}
	}
	if st.SkipThreshold == 0 {
		st.SkipThreshold = 3
	}
	j.st = st
	return nil
}
