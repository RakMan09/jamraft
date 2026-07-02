package raft

import (
	"context"
	"sync"
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
				n.beginCampaignLocked()
			}
			n.mu.Unlock()
		}
	}
}

// beginCampaignLocked starts a campaign. With pre-vote enabled it first runs a
// pre-vote round (which does NOT bump the term) so that a partitioned or
// recently-restarted node with a stale log cannot disrupt a healthy leader by
// forcing everyone to a higher term. Only after winning the pre-vote does it
// start a real election.
func (n *Node) beginCampaignLocked() {
	n.role = Candidate
	n.leaderID = ""
	n.resetElectionDeadline()

	term := n.currentTerm
	lastIdx := n.lastIndex()
	lastTerm := n.lastTerm()

	if n.cfg.preVoteEnabled() {
		n.logf("starting pre-vote for term %d", term+1)
		go n.campaign(term, lastIdx, lastTerm)
		return
	}
	// Pre-vote disabled: go straight to a real election.
	n.startRealElectionLocked(lastIdx, lastTerm)
}

// campaign runs the pre-vote round, then (if won) the real election. It runs
// outside the lock.
func (n *Node) campaign(baseTerm, lastIdx, lastTerm uint64) {
	if !n.gatherVotes(baseTerm+1, lastIdx, lastTerm, true) {
		return // pre-vote failed; we'll retry on the next election timeout
	}

	n.mu.Lock()
	// Make sure nothing changed underneath us during the pre-vote round.
	if n.role != Candidate || n.currentTerm != baseTerm {
		n.mu.Unlock()
		return
	}
	n.startRealElectionLocked(lastIdx, lastTerm)
	n.mu.Unlock()
}

// startRealElectionLocked increments the term, votes for self, and solicits real
// votes. Caller holds n.mu.
func (n *Node) startRealElectionLocked(lastIdx, lastTerm uint64) {
	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.id
	n.leaderID = ""
	n.persistHardState()
	n.resetElectionDeadline()
	term := n.currentTerm
	n.logf("starting election")

	if n.quorum <= 1 {
		n.becomeLeaderLocked()
		return
	}
	go n.gatherVotes(term, lastIdx, lastTerm, false)
}

// gatherVotes sends RequestVote (pre-vote or real) to all peers and returns
// whether a majority granted. For real votes, winning transitions to leader as
// a side effect. Runs outside the lock.
func (n *Node) gatherVotes(term, lastIdx, lastTerm uint64, preVote bool) bool {
	var (
		mu      sync.Mutex
		granted = 1 // self
		won     bool
		wg      sync.WaitGroup
	)
	for _, peer := range n.peers {
		peer := peer
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.ElectionTimeoutMax)
			defer cancel()
			req := &raftpb.RequestVoteRequest{
				Term:         term,
				CandidateId:  n.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
				PreVote:      preVote,
			}
			resp, err := n.cfg.Transport.RequestVote(ctx, peer, req)
			if err != nil || resp == nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()
			// A real reply with a higher term forces us to step down. (Pre-vote
			// replies never carry a higher term that we should adopt.)
			if !preVote && resp.Term > n.currentTerm {
				n.stepDown(resp.Term)
				return
			}
			if !preVote && (n.role != Candidate || n.currentTerm != term) {
				return // stale
			}
			if resp.VoteGranted {
				mu.Lock()
				granted++
				reachedQuorum := granted >= n.quorum && !won
				if reachedQuorum {
					won = true
				}
				mu.Unlock()
				if reachedQuorum && !preVote {
					n.becomeLeaderLocked()
				}
			}
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	return granted >= n.quorum
}

// RequestVote handles an incoming RequestVote RPC.
func (n *Node) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &raftpb.RequestVoteResponse{Term: n.currentTerm}
	if req.Term < n.currentTerm {
		return resp, nil
	}

	// Pre-vote: answer whether we WOULD grant, without mutating any state. Deny
	// if we've heard from a leader recently (leader stickiness), which is what
	// prevents a disruptive node from starting elections while a leader is live.
	if req.PreVote {
		granted := req.Term >= n.currentTerm &&
			n.candidateUpToDate(req) &&
			time.Since(n.lastLeaderContact) >= n.cfg.ElectionTimeoutMin
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
	if n.role == Leader {
		return
	}
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
