package raft

import (
	"context"
	"encoding/json"
	"time"

	"github.com/rakman09/jamraft/internal/jukebox"
	raftpb "github.com/rakman09/jamraft/proto/raftpb"
)

func noopCommand() []byte {
	b, _ := json.Marshal(jukebox.Command{Op: jukebox.OpNoop})
	return b
}

// triggerReplication wakes every peer's replication loop (caller holds n.mu).
func (n *Node) triggerReplication() {
	for _, ch := range n.triggerCh {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// replicationLoop runs on the leader, one goroutine per peer. It sends
// AppendEntries (or InstallSnapshot) either when signalled by a new proposal or
// on the heartbeat interval.
func (n *Node) replicationLoop(ctx context.Context, peer string) {
	n.mu.Lock()
	trigger := n.triggerCh[peer]
	n.mu.Unlock()

	timer := time.NewTimer(n.cfg.HeartbeatInterval)
	defer timer.Stop()
	for {
		n.sendAppendEntries(ctx, peer)
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(n.cfg.HeartbeatInterval)
		select {
		case <-ctx.Done():
			return
		case <-n.stopCh:
			return
		case <-trigger:
		case <-timer.C:
		}
	}
}

// sendAppendEntries sends one AppendEntries (or InstallSnapshot) RPC to peer and
// processes the reply.
func (n *Node) sendAppendEntries(ctx context.Context, peer string) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	ni := n.nextIndex[peer]

	// If the peer needs entries we've already compacted, ship a snapshot.
	if ni <= n.snapshotIndex {
		n.mu.Unlock()
		n.sendSnapshot(ctx, peer, term)
		return
	}

	prevIndex := ni - 1
	prevTerm, ok := n.termAt(prevIndex)
	if !ok {
		// prevIndex is below the snapshot; fall back to snapshot install.
		n.mu.Unlock()
		n.sendSnapshot(ctx, peer, term)
		return
	}
	entries := n.sliceFrom(ni)
	req := &raftpb.AppendEntriesRequest{
		Term:         term,
		LeaderId:     n.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	rctx, cancel := context.WithTimeout(ctx, n.cfg.ElectionTimeoutMax)
	defer cancel()
	resp, err := n.cfg.Transport.AppendEntries(rctx, peer, req)
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
		return // stale
	}

	if resp.Success {
		newMatch := prevIndex + uint64(len(entries))
		if newMatch > n.matchIndex[peer] {
			n.matchIndex[peer] = newMatch
			n.nextIndex[peer] = newMatch + 1
			n.advanceCommitLocked()
		}
		return
	}

	// Rejected: back up nextIndex using the follower's conflict hint.
	n.nextIndex[peer] = n.nextIndexAfterConflict(resp)
	if n.nextIndex[peer] < 1 {
		n.nextIndex[peer] = 1
	}
	// Retry promptly.
	if ch, ok := n.triggerCh[peer]; ok {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// nextIndexAfterConflict computes the new nextIndex for a peer given its
// conflict hints (an optimization over decrement-by-one).
func (n *Node) nextIndexAfterConflict(resp *raftpb.AppendEntriesResponse) uint64 {
	if resp.ConflictTerm == 0 {
		// Follower's log is shorter than prevLogIndex.
		if resp.ConflictIndex == 0 {
			return 1
		}
		return resp.ConflictIndex
	}
	// If we have entries in ConflictTerm, skip to just past our last such entry.
	lastOfTerm := uint64(0)
	for _, e := range n.entries {
		if e.Term == resp.ConflictTerm {
			lastOfTerm = e.Index
		}
	}
	if lastOfTerm > 0 {
		return lastOfTerm + 1
	}
	return resp.ConflictIndex
}

// advanceCommitLocked advances commitIndex to the highest index replicated on a
// majority, but ONLY for entries from the current term (Raft's commit rule).
func (n *Node) advanceCommitLocked() {
	for idx := n.lastIndex(); idx > n.commitIndex; idx-- {
		termOfIdx, ok := n.termAt(idx)
		if !ok || termOfIdx != n.currentTerm {
			continue // never commit prior-term entries by direct count
		}
		count := 1 // self
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= idx {
				count++
			}
		}
		if count >= n.quorum {
			n.commitIndex = idx
			n.signalApply()
			return
		}
	}
}

// AppendEntries handles an incoming AppendEntries RPC (heartbeat when Entries is
// empty).
func (n *Node) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &raftpb.AppendEntriesResponse{Term: n.currentTerm}
	if req.Term < n.currentTerm {
		return resp, nil
	}

	// A valid leader for this term exists; (re)become follower and reset timer.
	if req.Term > n.currentTerm {
		n.stepDown(req.Term)
	}
	n.role = Follower
	n.leaderID = req.LeaderId
	n.resetElectionDeadline()
	resp.Term = n.currentTerm

	// Consistency check on prevLogIndex/prevLogTerm.
	if req.PrevLogIndex > n.lastIndex() {
		resp.ConflictIndex = n.lastIndex() + 1
		resp.ConflictTerm = 0
		return resp, nil
	}
	if req.PrevLogIndex >= n.snapshotIndex {
		if t, ok := n.termAt(req.PrevLogIndex); !ok || t != req.PrevLogTerm {
			resp.ConflictTerm = t
			resp.ConflictIndex = n.firstIndexOfTerm(t)
			return resp, nil
		}
	}
	// If PrevLogIndex < snapshotIndex, everything up to snapshotIndex is already
	// committed and consistent; we simply skip those entries below.

	// Merge entries, truncating on the first real conflict.
	var toPersist []*raftpb.LogEntry
	truncated := false
	for _, e := range req.Entries {
		if e.Index <= n.snapshotIndex {
			continue
		}
		if existing, ok := n.termAt(e.Index); ok {
			if existing == e.Term {
				continue // already have a matching entry
			}
			// Conflict: delete this and everything after it.
			n.entries = n.entries[:e.Index-n.snapshotIndex]
			n.persistTruncateSuffix(e.Index)
			truncated = true
		}
		n.entries = append(n.entries, e)
		toPersist = append(toPersist, e)
	}
	if len(toPersist) > 0 {
		n.persistAppend(toPersist)
	}
	_ = truncated

	// Advance commit index (persist-before-apply is handled by apply loop).
	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = min64(req.LeaderCommit, n.lastIndex())
		n.signalApply()
	}

	resp.Success = true
	return resp, nil
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
