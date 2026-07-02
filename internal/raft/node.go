// Package raft is a from-scratch implementation of the Raft consensus
// algorithm (Ongaro & Ousterhout, "In Search of an Understandable Consensus
// Algorithm"), following figure 2 of the paper. It is deliberately independent
// of any transport or storage implementation: those are injected via the
// Transport and store.Storage interfaces so the same core can run over gRPC in
// production and over an in-process deterministic simulator in tests.
package raft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/rakman09/jamraft/internal/store"
	raftpb "github.com/rakman09/jamraft/proto/raftpb"
)

// Role is a node's current Raft role.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// Errors returned by client-facing operations.
var (
	ErrNotLeader       = errors.New("not leader")
	ErrLostLeadership  = errors.New("lost leadership before commit")
	ErrStopped         = errors.New("node stopped")
	ErrTimeout         = errors.New("operation timed out")
)

// StateMachine is the application the log drives (here, the jukebox).
type StateMachine interface {
	// Apply applies one committed command and returns a result payload.
	Apply(cmd []byte) []byte
	// Snapshot returns a serialized image of the entire application state.
	Snapshot() []byte
	// Restore replaces application state from a snapshot.
	Restore(data []byte) error
}

// Transport is the client side of inter-node RPC. Each method targets a peer id.
type Transport interface {
	RequestVote(ctx context.Context, target string, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error)
	AppendEntries(ctx context.Context, target string, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error)
	InstallSnapshot(ctx context.Context, target string, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error)
}

// Config configures a Raft node.
type Config struct {
	ID           string
	Peers        []string // all node ids in the cluster, including this node
	Storage      store.Storage
	Transport    Transport
	StateMachine StateMachine

	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	SnapshotThreshold  int // take a snapshot after the log exceeds this many entries

	// DisablePreVote turns off the pre-vote extension. Pre-vote is ON by default
	// (zero value) because it prevents a partitioned or recently-restarted node
	// with a stale log from disrupting a healthy leader.
	DisablePreVote bool

	Logger *log.Logger
	Rand   *rand.Rand
}

// PreVote reports whether the pre-vote extension is enabled.
func (c *Config) preVoteEnabled() bool { return !c.DisablePreVote }

func (c *Config) withDefaults() {
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 150 * time.Millisecond
	}
	if c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMax = 300 * time.Millisecond
	}
	if c.SnapshotThreshold == 0 {
		c.SnapshotThreshold = 1000
	}
	if c.Logger == nil {
		c.Logger = log.New(io.Discard, "", 0)
	}
	if c.Rand == nil {
		c.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
}

type waiter struct {
	term uint64
	ch   chan proposeResult
}

type proposeResult struct {
	result []byte
	err    error
}

// Node is a single Raft server.
type Node struct {
	mu     sync.Mutex
	cfg    Config
	id     string
	peers  []string // peer ids EXCLUDING self
	quorum int

	// Persistent state (persisted before responding to RPCs).
	currentTerm uint64
	votedFor    string

	// Log. entries[0] is a sentinel holding {Index: snapshotIndex, Term:
	// snapshotTerm} so that index math near the snapshot boundary is uniform.
	entries       []*raftpb.LogEntry
	snapshotIndex uint64
	snapshotTerm  uint64
	snapshotData  []byte

	// Volatile state.
	role        Role
	leaderID    string
	commitIndex uint64
	lastApplied uint64

	// Leader volatile state.
	nextIndex  map[string]uint64
	matchIndex map[string]uint64
	triggerCh  map[string]chan struct{}

	// Election timing.
	electionDeadline  time.Time
	lastLeaderContact time.Time // last valid AppendEntries/InstallSnapshot from a leader

	// apply loop signalling and client proposal waiters.
	applyCh chan struct{}
	pending map[uint64]*waiter

	// lifecycle
	stopCh       chan struct{}
	wg           sync.WaitGroup
	running      bool
	leaderCtx    context.Context
	leaderCancel context.CancelFunc

	// observability
	appliedCount uint64
}

// New creates a Node, loading any persisted state from storage.
func New(cfg Config) (*Node, error) {
	cfg.withDefaults()
	if cfg.Storage == nil || cfg.Transport == nil || cfg.StateMachine == nil {
		return nil, errors.New("raft: Storage, Transport and StateMachine are required")
	}
	n := &Node{
		cfg:        cfg,
		id:         cfg.ID,
		role:       Follower,
		nextIndex:  map[string]uint64{},
		matchIndex: map[string]uint64{},
		triggerCh:  map[string]chan struct{}{},
		applyCh:    make(chan struct{}, 1),
		pending:    map[uint64]*waiter{},
		stopCh:     make(chan struct{}),
	}
	for _, p := range cfg.Peers {
		if p != cfg.ID {
			n.peers = append(n.peers, p)
		}
	}
	n.quorum = (len(cfg.Peers) / 2) + 1

	if err := n.loadFromStorage(); err != nil {
		return nil, err
	}
	return n, nil
}

// loadFromStorage rebuilds in-memory state from durable storage.
func (n *Node) loadFromStorage() error {
	hs, err := n.cfg.Storage.LoadHardState()
	if err != nil {
		return err
	}
	n.currentTerm = hs.Term
	n.votedFor = hs.VotedFor

	snap, err := n.cfg.Storage.LoadSnapshot()
	if err != nil {
		return err
	}
	n.snapshotIndex = snap.LastIncludedIndex
	n.snapshotTerm = snap.LastIncludedTerm
	n.snapshotData = snap.Data
	if len(snap.Data) > 0 {
		if err := n.cfg.StateMachine.Restore(snap.Data); err != nil {
			return fmt.Errorf("restore snapshot: %w", err)
		}
	}
	// Sentinel entry at the snapshot boundary.
	n.entries = []*raftpb.LogEntry{{Index: n.snapshotIndex, Term: n.snapshotTerm}}

	persisted, err := n.cfg.Storage.AllEntries()
	if err != nil {
		return err
	}
	for _, e := range persisted {
		if e.Index > n.snapshotIndex {
			n.entries = append(n.entries, e)
		}
	}
	n.commitIndex = n.snapshotIndex
	n.lastApplied = n.snapshotIndex
	return nil
}

// Start launches the node's background goroutines.
func (n *Node) Start() {
	n.mu.Lock()
	if n.running {
		n.mu.Unlock()
		return
	}
	n.running = true
	n.resetElectionDeadline()
	n.logf("started (lastIndex=%d, commit=%d)", n.lastIndex(), n.commitIndex)
	n.mu.Unlock()

	n.wg.Add(2)
	go n.electionLoop()
	go n.applyLoop()
}

// Stop halts the node and waits for background goroutines to exit.
func (n *Node) Stop() {
	n.mu.Lock()
	if !n.running {
		n.mu.Unlock()
		return
	}
	n.running = false
	close(n.stopCh)
	if n.leaderCancel != nil {
		n.leaderCancel()
	}
	// Fail any pending proposals.
	for idx, w := range n.pending {
		w.ch <- proposeResult{err: ErrStopped}
		delete(n.pending, idx)
	}
	n.mu.Unlock()
	n.wg.Wait()
}

// ---- log helpers (caller must hold n.mu) ----

func (n *Node) lastIndex() uint64 { return n.entries[len(n.entries)-1].Index }
func (n *Node) lastTerm() uint64  { return n.entries[len(n.entries)-1].Term }

// termAt returns the term of the entry at index, if it is present in the log
// (including the snapshot boundary sentinel).
func (n *Node) termAt(index uint64) (uint64, bool) {
	if index < n.snapshotIndex {
		return 0, false
	}
	off := index - n.snapshotIndex
	if off >= uint64(len(n.entries)) {
		return 0, false
	}
	return n.entries[off].Term, true
}

// entryAt returns the entry at index (index must be > snapshotIndex).
func (n *Node) entryAt(index uint64) *raftpb.LogEntry {
	return n.entries[index-n.snapshotIndex]
}

// sliceFrom returns copies of entries with index >= from (from > snapshotIndex).
func (n *Node) sliceFrom(from uint64) []*raftpb.LogEntry {
	if from <= n.snapshotIndex {
		from = n.snapshotIndex + 1
	}
	off := from - n.snapshotIndex
	if off >= uint64(len(n.entries)) {
		return nil
	}
	src := n.entries[off:]
	out := make([]*raftpb.LogEntry, len(src))
	copy(out, src)
	return out
}

// firstIndexOfTerm returns the first log index whose entry has the given term.
func (n *Node) firstIndexOfTerm(term uint64) uint64 {
	for _, e := range n.entries {
		if e.Term == term {
			return e.Index
		}
	}
	return n.snapshotIndex + 1
}

// ---- persistence helpers (caller must hold n.mu) ----

func (n *Node) persistHardState() {
	if err := n.cfg.Storage.SaveHardState(store.HardState{Term: n.currentTerm, VotedFor: n.votedFor}); err != nil {
		n.logf("ERROR persist hard state: %v", err)
	}
}

func (n *Node) persistAppend(entries []*raftpb.LogEntry) {
	if err := n.cfg.Storage.Append(entries); err != nil {
		n.logf("ERROR persist append: %v", err)
	}
}

func (n *Node) persistTruncateSuffix(from uint64) {
	if err := n.cfg.Storage.TruncateSuffix(from); err != nil {
		n.logf("ERROR persist truncate: %v", err)
	}
}

// ---- role transitions (caller must hold n.mu) ----

func (n *Node) resetElectionDeadline() {
	d := n.cfg.ElectionTimeoutMin +
		time.Duration(n.cfg.Rand.Int63n(int64(n.cfg.ElectionTimeoutMax-n.cfg.ElectionTimeoutMin+1)))
	n.electionDeadline = time.Now().Add(d)
}

// stepDown converts to follower at the given term (>= currentTerm).
func (n *Node) stepDown(term uint64) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
		n.persistHardState()
	}
	if n.role == Leader {
		n.logf("stepping down from leader (term=%d)", n.currentTerm)
	}
	n.role = Follower
	if n.leaderCancel != nil {
		n.leaderCancel()
		n.leaderCancel = nil
		n.leaderCtx = nil
	}
	// Any in-flight proposals cannot commit under our leadership anymore.
	n.failPending(ErrLostLeadership)
}

func (n *Node) failPending(err error) {
	for idx, w := range n.pending {
		select {
		case w.ch <- proposeResult{err: err}:
		default:
		}
		delete(n.pending, idx)
	}
}

func (n *Node) logf(format string, args ...any) {
	n.cfg.Logger.Printf("[%s term=%d %s] "+format, append([]any{n.id, n.currentTerm, n.role}, args...)...)
}

// ---- apply loop ----

func (n *Node) signalApply() {
	select {
	case n.applyCh <- struct{}{}:
	default:
	}
}

func (n *Node) applyLoop() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applyCh:
		}
		n.applyReady()
	}
}

func (n *Node) applyReady() {
	n.mu.Lock()
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		idx := n.lastApplied
		if idx <= n.snapshotIndex {
			continue
		}
		e := n.entryAt(idx)
		result := n.cfg.StateMachine.Apply(e.Command)
		n.appliedCount++
		if w, ok := n.pending[idx]; ok {
			if w.term == e.Term {
				w.ch <- proposeResult{result: result}
			} else {
				w.ch <- proposeResult{err: ErrLostLeadership}
			}
			delete(n.pending, idx)
		}
	}
	n.maybeSnapshotLocked()
	n.mu.Unlock()
}

// ---- proposals ----

// Propose appends a command to the leader's log and returns its index/term.
// It does not wait for commit. Returns isLeader=false if this node is not the
// leader.
func (n *Node) Propose(cmd []byte) (index uint64, term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return 0, 0, false
	}
	e := &raftpb.LogEntry{Term: n.currentTerm, Index: n.lastIndex() + 1, Command: cmd}
	n.entries = append(n.entries, e)
	n.persistAppend([]*raftpb.LogEntry{e})
	n.advanceCommitLocked() // commits immediately in a single-node cluster
	n.triggerReplication()
	return e.Index, e.Term, true
}

// Submit proposes a command and blocks until it is committed and applied, or
// the context is cancelled. It returns the state machine result.
func (n *Node) Submit(ctx context.Context, cmd []byte) ([]byte, error) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return nil, ErrNotLeader
	}
	e := &raftpb.LogEntry{Term: n.currentTerm, Index: n.lastIndex() + 1, Command: cmd}
	n.entries = append(n.entries, e)
	n.persistAppend([]*raftpb.LogEntry{e})
	w := &waiter{term: e.Term, ch: make(chan proposeResult, 1)}
	n.pending[e.Index] = w
	n.advanceCommitLocked() // commits immediately in a single-node cluster
	n.triggerReplication()
	n.mu.Unlock()

	select {
	case <-ctx.Done():
		n.mu.Lock()
		delete(n.pending, e.Index)
		n.mu.Unlock()
		return nil, ctx.Err()
	case <-n.stopCh:
		return nil, ErrStopped
	case res := <-w.ch:
		return res.result, res.err
	}
}

// Status is a snapshot of node state for the API/UI.
type Status struct {
	ID          string `json:"id"`
	Term        uint64 `json:"term"`
	Role        string `json:"role"`
	LeaderID    string `json:"leaderId"`
	CommitIndex uint64 `json:"commitIndex"`
	LastApplied uint64 `json:"lastApplied"`
	LastIndex   uint64 `json:"lastIndex"`
	LogSize     int    `json:"logSize"`
	SnapshotIdx uint64 `json:"snapshotIndex"`
}

// Status returns a consistent snapshot of the node's Raft state.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID:          n.id,
		Term:        n.currentTerm,
		Role:        n.role.String(),
		LeaderID:    n.leaderID,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
		LastIndex:   n.lastIndex(),
		LogSize:     len(n.entries) - 1,
		SnapshotIdx: n.snapshotIndex,
	}
}

// IsLeader reports whether the node currently believes it is leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == Leader
}

// LeaderID returns the last known leader id ("" if unknown).
func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// ID returns this node's id.
func (n *Node) ID() string { return n.id }
