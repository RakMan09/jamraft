package raft

import (
	"context"
	"time"

	"github.com/rakman09/jamraft/internal/store"
	raftpb "github.com/rakman09/jamraft/proto/raftpb"
)

// maybeSnapshotLocked compacts the log if it has grown past the threshold. It
// captures the state machine at lastApplied, truncates the log prefix, and keeps
// only entries after the snapshot point. Caller holds n.mu.
func (n *Node) maybeSnapshotLocked() {
	if n.cfg.SnapshotThreshold <= 0 {
		return
	}
	if len(n.entries)-1 <= n.cfg.SnapshotThreshold {
		return
	}
	if n.lastApplied <= n.snapshotIndex {
		return
	}

	snapIndex := n.lastApplied
	snapTerm, ok := n.termAt(snapIndex)
	if !ok {
		return
	}
	data := n.cfg.StateMachine.Snapshot()

	if err := n.cfg.Storage.SaveSnapshot(store.Snapshot{
		LastIncludedIndex: snapIndex,
		LastIncludedTerm:  snapTerm,
		Data:              data,
	}); err != nil {
		n.logf("ERROR save snapshot: %v", err)
		return
	}
	n.persistTruncatePrefix(snapIndex)

	kept := n.sliceFrom(snapIndex + 1)
	n.entries = append([]*raftpb.LogEntry{{Index: snapIndex, Term: snapTerm}}, kept...)
	n.snapshotIndex = snapIndex
	n.snapshotTerm = snapTerm
	n.snapshotData = data
	n.logf("snapshot taken at index=%d, log compacted", snapIndex)
}

func (n *Node) persistTruncatePrefix(idx uint64) {
	if err := n.cfg.Storage.TruncatePrefix(idx); err != nil {
		n.logf("ERROR persist truncate prefix: %v", err)
	}
}

// sendSnapshot ships the current snapshot to a lagging follower.
func (n *Node) sendSnapshot(ctx context.Context, peer string, term uint64) {
	n.mu.Lock()
	if n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	req := &raftpb.InstallSnapshotRequest{
		Term:              term,
		LeaderId:          n.id,
		LastIncludedIndex: n.snapshotIndex,
		LastIncludedTerm:  n.snapshotTerm,
		Data:              n.snapshotData,
	}
	n.mu.Unlock()

	rctx, cancel := context.WithTimeout(ctx, n.cfg.ElectionTimeoutMax)
	defer cancel()
	resp, err := n.cfg.Transport.InstallSnapshot(rctx, peer, req)
	if err != nil || resp == nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if resp.Term > n.currentTerm {
		n.stepDown(resp.Term)
		return
	}
	if n.role != Leader || n.currentTerm != term {
		return
	}
	if req.LastIncludedIndex > n.matchIndex[peer] {
		n.matchIndex[peer] = req.LastIncludedIndex
		n.nextIndex[peer] = req.LastIncludedIndex + 1
	}
}

// InstallSnapshot handles an incoming InstallSnapshot RPC.
func (n *Node) InstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &raftpb.InstallSnapshotResponse{Term: n.currentTerm}
	if req.Term < n.currentTerm {
		return resp, nil
	}
	if req.Term > n.currentTerm {
		n.stepDown(req.Term)
	}
	n.role = Follower
	n.leaderID = req.LeaderId
	n.lastLeaderContact = time.Now()
	n.resetElectionDeadline()
	resp.Term = n.currentTerm

	// Ignore snapshots we already cover.
	if req.LastIncludedIndex <= n.snapshotIndex {
		return resp, nil
	}

	if err := n.cfg.StateMachine.Restore(req.Data); err != nil {
		n.logf("ERROR restore snapshot: %v", err)
		return resp, nil
	}

	// Keep any suffix of our log that is consistent with the snapshot; otherwise
	// discard the whole log.
	var kept []*raftpb.LogEntry
	if t, ok := n.termAt(req.LastIncludedIndex); ok && t == req.LastIncludedTerm {
		kept = n.sliceFrom(req.LastIncludedIndex + 1)
	} else {
		n.persistTruncateSuffix(n.snapshotIndex + 1) // drop conflicting suffix
	}

	if err := n.cfg.Storage.SaveSnapshot(store.Snapshot{
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              req.Data,
	}); err != nil {
		n.logf("ERROR save installed snapshot: %v", err)
	}
	n.persistTruncatePrefix(req.LastIncludedIndex)

	n.entries = append([]*raftpb.LogEntry{{Index: req.LastIncludedIndex, Term: req.LastIncludedTerm}}, kept...)
	n.snapshotIndex = req.LastIncludedIndex
	n.snapshotTerm = req.LastIncludedTerm
	n.snapshotData = req.Data

	if req.LastIncludedIndex > n.commitIndex {
		n.commitIndex = req.LastIncludedIndex
	}
	if req.LastIncludedIndex > n.lastApplied {
		n.lastApplied = req.LastIncludedIndex
	}
	n.logf("installed snapshot up to index=%d", req.LastIncludedIndex)
	return resp, nil
}
