package raft

import (
	"context"
	"time"

	raftpb "github.com/rakman09/jamraft/proto/raftpb"
)

// electionLoop drives election timeouts. A follower or candidate that has not
// heard from a valid leader (or granted a vote) within a randomized timeout
// starts a new election. Randomized timeouts make split votes rare.
func (n *Node) electionLoop() {
	defer n.wg.Done()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			if n.role != Leader && time.Now().After(n.electionDeadline) {
				n.startElectionLocked()
			}
			n.mu.Unlock()
		}
	}
}

// startElectionLocked transitions to candidate and solicits votes.
func (n *Node) startElectionLocked() {
	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.id
	n.leaderID = ""
	n.persistHardState()
	n.resetElectionDeadline()

	term := n.currentTerm
	lastIdx := n.lastIndex()
	lastTerm := n.lastTerm()
	n.logf("starting election")

	votes := 1 // vote for self
	var votesMu = &voteCounter{granted: votes}

	// Single-node cluster (or self-vote already a majority): win immediately.
	if votes >= n.quorum {
		votesMu.done = true
		n.becomeLeaderLocked()
		return
	}

	for _, peer := range n.peers {
		peer := peer
		req := &raftpb.RequestVoteRequest{
			Term:         term,
			CandidateId:  n.id,
			LastLogIndex: lastIdx,
			LastLogTerm:  lastTerm,
		}
		go n.solicitVote(peer, term, req, votesMu)
	}
}

type voteCounter struct {
	granted int
	done    bool
}

func (n *Node) solicitVote(peer string, term uint64, req *raftpb.RequestVoteRequest, vc *voteCounter) {
	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.ElectionTimeoutMax)
	defer cancel()
	resp, err := n.cfg.Transport.RequestVote(ctx, peer, req)
	if err != nil || resp == nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.currentTerm {
		n.stepDown(resp.Term)
		return
	}
	// Ignore stale replies (term moved on, or we're no longer a candidate).
	if n.role != Candidate || n.currentTerm != term {
		return
	}
	if resp.VoteGranted {
		vc.granted++
		if !vc.done && vc.granted >= n.quorum {
			vc.done = true
			n.becomeLeaderLocked()
		}
	}
}

// RequestVote handles an incoming RequestVote RPC.
func (n *Node) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &raftpb.RequestVoteResponse{Term: n.currentTerm}

	if req.Term < n.currentTerm {
		return resp, nil
	}

	// Pre-vote (stretch): answer whether we would grant, without mutating state.
	if req.PreVote {
		granted := req.Term >= n.currentTerm && n.candidateUpToDate(req) && time.Now().After(n.electionDeadline.Add(-n.cfg.ElectionTimeoutMin))
		resp.Term = n.currentTerm
		resp.VoteGranted = granted
		return resp, nil
	}

	if req.Term > n.currentTerm {
		n.stepDown(req.Term)
	}
	resp.Term = n.currentTerm

	if (n.votedFor == "" || n.votedFor == req.CandidateId) && n.candidateUpToDate(req) {
		n.votedFor = req.CandidateId
		n.persistHardState() // persist the vote BEFORE replying (safety!)
		n.resetElectionDeadline()
		resp.VoteGranted = true
		n.logf("granted vote to %s", req.CandidateId)
	}
	return resp, nil
}

// candidateUpToDate implements Raft's election restriction: the candidate's log
// must be at least as up-to-date as ours (higher last term wins; on a tie, the
// longer log wins).
func (n *Node) candidateUpToDate(req *raftpb.RequestVoteRequest) bool {
	myLastTerm := n.lastTerm()
	myLastIndex := n.lastIndex()
	if req.LastLogTerm != myLastTerm {
		return req.LastLogTerm > myLastTerm
	}
	return req.LastLogIndex >= myLastIndex
}

// becomeLeaderLocked transitions the node to leader for the current term.
func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	n.leaderID = n.id
	n.nextIndex = map[string]uint64{}
	n.matchIndex = map[string]uint64{}
	n.triggerCh = map[string]chan struct{}{}
	next := n.lastIndex() + 1
	for _, peer := range n.peers {
		n.nextIndex[peer] = next
		n.matchIndex[peer] = 0
		n.triggerCh[peer] = make(chan struct{}, 1)
	}
	n.leaderCtx, n.leaderCancel = context.WithCancel(context.Background())
	n.logf("became LEADER")

	// Append a no-op entry for the new term. Committing an entry from the
	// current term is what lets the leader safely advance the commit index over
	// entries inherited from previous terms (the commit-rule subtlety).
	noop := &raftpb.LogEntry{Term: n.currentTerm, Index: n.lastIndex() + 1, Command: noopCommand()}
	n.entries = append(n.entries, noop)
	n.persistAppend([]*raftpb.LogEntry{noop})
	n.advanceCommitLocked() // commits immediately in a single-node cluster

	for _, peer := range n.peers {
		go n.replicationLoop(n.leaderCtx, peer)
	}
	n.triggerReplication()
}
