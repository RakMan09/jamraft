package raft

import (
	"context"
	"sync"
	"time"

	raftpb "github.com/rakman09/jamraft/proto/raftpb"
)

// LinearizableRead performs a read-index read. It guarantees the read reflects
// all writes committed before the call returned, without going through the log.
//
// The steps (Raft §6.4):
//  1. Record the current commit index as the read index.
//  2. Confirm we are still the leader by exchanging heartbeats with a majority
//     (a deposed leader in a minority partition cannot get a quorum, so it will
//     fail here rather than serve stale data).
//  3. Wait until the state machine has applied up to the read index.
//
// It returns the read index reached, or an error if leadership could not be
// confirmed.
func (n *Node) LinearizableRead(ctx context.Context) (uint64, error) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	// The no-op appended on election ensures the leader has a current-term entry
	// to commit, so commitIndex reflects the true committed prefix.
	readIndex := n.commitIndex
	term := n.currentTerm
	n.mu.Unlock()

	if !n.confirmLeadership(ctx, term) {
		return 0, ErrNotLeader
	}

	if err := n.waitApplied(ctx, readIndex); err != nil {
		return 0, err
	}
	return readIndex, nil
}

// confirmLeadership sends an empty AppendEntries round and returns true if a
// majority (including self) acknowledges us as leader in the given term.
func (n *Node) confirmLeadership(ctx context.Context, term uint64) bool {
	n.mu.Lock()
	if n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return false
	}
	peers := append([]string(nil), n.peers...)
	commit := n.commitIndex
	n.mu.Unlock()

	if len(peers) == 0 {
		return true // single-node cluster
	}

	rctx, cancel := context.WithTimeout(ctx, n.cfg.ElectionTimeoutMax)
	defer cancel()

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		acks = 1 // self
	)
	for _, peer := range peers {
		peer := peer
		wg.Add(1)
		go func() {
			defer wg.Done()
			n.mu.Lock()
			if n.role != Leader || n.currentTerm != term {
				n.mu.Unlock()
				return
			}
			ni := n.nextIndex[peer]
			prevIndex := ni - 1
			prevTerm, ok := n.termAt(prevIndex)
			n.mu.Unlock()
			if !ok {
				prevIndex = 0
				prevTerm = 0
			}
			req := &raftpb.AppendEntriesRequest{
				Term:         term,
				LeaderId:     n.id,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
				LeaderCommit: commit,
			}
			resp, err := n.cfg.Transport.AppendEntries(rctx, peer, req)
			if err != nil || resp == nil {
				return
			}
			if resp.Term > term {
				n.mu.Lock()
				if resp.Term > n.currentTerm {
					n.stepDown(resp.Term)
				}
				n.mu.Unlock()
				return
			}
			if resp.Success {
				mu.Lock()
				acks++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	return acks >= n.quorum
}

// waitApplied blocks until lastApplied >= idx or the context is cancelled.
func (n *Node) waitApplied(ctx context.Context, idx uint64) error {
	for {
		n.mu.Lock()
		applied := n.lastApplied
		n.mu.Unlock()
		if applied >= idx {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-n.stopCh:
			return ErrStopped
		case <-time.After(2 * time.Millisecond):
		}
	}
}
