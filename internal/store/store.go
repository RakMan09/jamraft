// Package store provides durable persistence for Raft: hard state
// (currentTerm, votedFor), the replicated log, and the latest snapshot.
package store

import (
	raftpb "github.com/rakman09/jamraft/proto/raftpb"
)

// HardState is the Raft state that MUST be persisted before responding to RPCs.
type HardState struct {
	Term     uint64
	VotedFor string
}

// Snapshot is a compacted state-machine image plus the log position it covers.
type Snapshot struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

// Storage is the durable persistence contract the Raft core depends on.
//
// Implementations must be safe for use by a single Raft node (the node
// serializes its own access under its mutex).
type Storage interface {
	// SaveHardState durably persists currentTerm and votedFor.
	SaveHardState(hs HardState) error
	// LoadHardState returns the persisted hard state (zero value if none).
	LoadHardState() (HardState, error)

	// Append persists entries at the tail of the log. Entries are contiguous
	// with strictly increasing indexes.
	Append(entries []*raftpb.LogEntry) error
	// TruncateSuffix deletes every entry with index >= idx.
	TruncateSuffix(idx uint64) error
	// TruncatePrefix deletes every entry with index <= idx (post-snapshot).
	TruncatePrefix(idx uint64) error
	// AllEntries returns all persisted entries in ascending index order.
	AllEntries() ([]*raftpb.LogEntry, error)

	// SaveSnapshot persists a snapshot, replacing any previous one.
	SaveSnapshot(snap Snapshot) error
	// LoadSnapshot returns the persisted snapshot (zero value if none).
	LoadSnapshot() (Snapshot, error)

	Close() error
}
