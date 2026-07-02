package transport

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	raftpb "github.com/rakman09/jamraft/proto/raftpb"
)

// ErrUnreachable is returned when the simulated network drops or blocks a
// message.
var ErrUnreachable = errors.New("transport: peer unreachable")

// Network is an in-process transport that routes RPCs directly to registered
// node handlers, with controllable faults. All methods are safe for concurrent
// use.
type Network struct {
	mu       sync.Mutex
	nodes    map[string]Handler
	isolated map[string]bool
	group    map[string]int // partition group per node (0 == default)
	dropRate float64
	dupRate  float64
	minDelay time.Duration
	maxDelay time.Duration
	rng      *rand.Rand

	delivered uint64
	dropped   uint64
}

// NewNetwork creates an empty simulated network seeded deterministically.
func NewNetwork(seed int64) *Network {
	return &Network{
		nodes:    map[string]Handler{},
		isolated: map[string]bool{},
		group:    map[string]int{},
		rng:      rand.New(rand.NewSource(seed)),
	}
}

// Register adds or replaces the handler for a node id (used on (re)start).
func (net *Network) Register(id string, h Handler) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.nodes[id] = h
}

// Deregister removes a node handler (used on crash).
func (net *Network) Deregister(id string) {
	net.mu.Lock()
	defer net.mu.Unlock()
	delete(net.nodes, id)
}

// SetDropRate sets the probability [0,1] of dropping any given message.
func (net *Network) SetDropRate(p float64) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.dropRate = p
}

// SetDuplicateRate sets the probability [0,1] of delivering a message twice.
func (net *Network) SetDuplicateRate(p float64) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.dupRate = p
}

// SetLatency sets the per-message delay range.
func (net *Network) SetLatency(min, max time.Duration) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.minDelay, net.maxDelay = min, max
}

// Isolate cuts a node off from all others (both directions).
func (net *Network) Isolate(id string) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.isolated[id] = true
}

// Rejoin reconnects a previously isolated node.
func (net *Network) Rejoin(id string) {
	net.mu.Lock()
	defer net.mu.Unlock()
	delete(net.isolated, id)
}

// Partition splits the cluster into groups; nodes in different groups cannot
// communicate. Nodes not listed remain in the default group 0.
func (net *Network) Partition(groups ...[]string) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.group = map[string]int{}
	for gi, g := range groups {
		for _, id := range g {
			net.group[id] = gi + 1
		}
	}
}

// Heal removes all partitions and isolation.
func (net *Network) Heal() {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.group = map[string]int{}
	net.isolated = map[string]bool{}
}

// Stats returns delivered/dropped counters.
func (net *Network) Stats() (delivered, dropped uint64) {
	net.mu.Lock()
	defer net.mu.Unlock()
	return net.delivered, net.dropped
}

// canDeliver reports whether a message from -> to may be delivered, and applies
// random drop. Caller must hold net.mu.
func (net *Network) canDeliver(from, to string) bool {
	if net.isolated[from] || net.isolated[to] {
		return false
	}
	if net.group[from] != net.group[to] {
		return false
	}
	if net.dropRate > 0 && net.rng.Float64() < net.dropRate {
		return false
	}
	return true
}

func (net *Network) prepare(from, to string) (Handler, time.Duration, bool, bool) {
	net.mu.Lock()
	defer net.mu.Unlock()
	if !net.canDeliver(from, to) {
		net.dropped++
		return nil, 0, false, false
	}
	h, ok := net.nodes[to]
	if !ok {
		net.dropped++
		return nil, 0, false, false
	}
	var delay time.Duration
	if net.maxDelay > net.minDelay {
		delay = net.minDelay + time.Duration(net.rng.Int63n(int64(net.maxDelay-net.minDelay)))
	} else {
		delay = net.minDelay
	}
	dup := net.dupRate > 0 && net.rng.Float64() < net.dupRate
	net.delivered++
	return h, delay, dup, true
}

// Transport returns a raft.Transport bound to node id.
func (net *Network) Transport(id string) *Endpoint {
	return &Endpoint{net: net, id: id}
}

// Endpoint implements raft.Transport for a specific node.
type Endpoint struct {
	net *Network
	id  string
}

func (e *Endpoint) sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (e *Endpoint) RequestVote(ctx context.Context, target string, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	h, delay, _, ok := e.net.prepare(e.id, target)
	if !ok {
		return nil, ErrUnreachable
	}
	if !e.sleep(ctx, delay) {
		return nil, ctx.Err()
	}
	return h.RequestVote(ctx, req)
}

func (e *Endpoint) AppendEntries(ctx context.Context, target string, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	h, delay, dup, ok := e.net.prepare(e.id, target)
	if !ok {
		return nil, ErrUnreachable
	}
	if !e.sleep(ctx, delay) {
		return nil, ctx.Err()
	}
	resp, err := h.AppendEntries(ctx, req)
	if dup && err == nil {
		// Deliver a duplicate; AppendEntries is idempotent so the second reply
		// is discarded by the caller, but this exercises that idempotency.
		_, _ = h.AppendEntries(ctx, req)
	}
	return resp, err
}

func (e *Endpoint) InstallSnapshot(ctx context.Context, target string, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	h, delay, _, ok := e.net.prepare(e.id, target)
	if !ok {
		return nil, ErrUnreachable
	}
	if !e.sleep(ctx, delay) {
		return nil, ctx.Err()
	}
	return h.InstallSnapshot(ctx, req)
}
