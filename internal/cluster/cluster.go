// Package cluster wires together a set of in-process Raft nodes over the
// simulated network, backed by in-memory storage and the jukebox state machine.
// It is shared by the Raft unit tests and the chaos harness.
package cluster

import (
	"context"
	"io"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/rakman09/jamraft/internal/jukebox"
	"github.com/rakman09/jamraft/internal/raft"
	"github.com/rakman09/jamraft/internal/store"
	"github.com/rakman09/jamraft/internal/transport"
)

// Options tunes a simulated cluster.
type Options struct {
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	SnapshotThreshold  int
	Seed               int64
	Logger             *log.Logger
}

// DefaultOptions returns fast timeouts suitable for tests.
func DefaultOptions() Options {
	return Options{
		HeartbeatInterval:  20 * time.Millisecond,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		SnapshotThreshold:  1000,
		Seed:               1,
		Logger:             log.New(io.Discard, "", 0),
	}
}

// SimCluster is a set of Raft nodes connected via a Network simulator.
type SimCluster struct {
	mu     sync.Mutex
	Net    *transport.Network
	ids    []string
	opts   Options
	nodes  map[string]*raft.Node
	stores map[string]store.Storage
	sms    map[string]*jukebox.Jukebox
}

// New creates and starts a simulated cluster with the given node ids.
func New(ids []string, opts Options) *SimCluster {
	if opts.Logger == nil {
		opts.Logger = log.New(io.Discard, "", 0)
	}
	c := &SimCluster{
		Net:    transport.NewNetwork(opts.Seed),
		ids:    append([]string(nil), ids...),
		opts:   opts,
		nodes:  map[string]*raft.Node{},
		stores: map[string]store.Storage{},
		sms:    map[string]*jukebox.Jukebox{},
	}
	for _, id := range ids {
		c.stores[id] = store.NewMemStore()
	}
	for _, id := range ids {
		c.startNode(id)
	}
	return c
}

// startNode constructs and starts a node with a fresh state machine, replaying
// from its (persistent) store. Caller may hold or not hold c.mu depending on
// context; internal callers manage locking.
func (c *SimCluster) startNode(id string) {
	sm := jukebox.New()
	n, err := raft.New(raft.Config{
		ID:                 id,
		Peers:              c.ids,
		Storage:            c.stores[id],
		Transport:          c.Net.Transport(id),
		StateMachine:       sm,
		HeartbeatInterval:  c.opts.HeartbeatInterval,
		ElectionTimeoutMin: c.opts.ElectionTimeoutMin,
		ElectionTimeoutMax: c.opts.ElectionTimeoutMax,
		SnapshotThreshold:  c.opts.SnapshotThreshold,
		Logger:             c.opts.Logger,
		Rand:               rand.New(rand.NewSource(c.opts.Seed + int64(len(id)) + int64(id[len(id)-1]))),
	})
	if err != nil {
		panic(err)
	}
	c.nodes[id] = n
	c.sms[id] = sm
	c.Net.Register(id, n)
	n.Start()
}

// IDs returns the node ids.
func (c *SimCluster) IDs() []string { return append([]string(nil), c.ids...) }

// Node returns the node with the given id (may be nil if crashed).
func (c *SimCluster) Node(id string) *raft.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

// StateMachine returns the jukebox for a node id.
func (c *SimCluster) StateMachine(id string) *jukebox.Jukebox {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sms[id]
}

// Crash stops a node and disconnects it, but keeps its durable store so it can
// be restarted.
func (c *SimCluster) Crash(id string) {
	c.mu.Lock()
	n := c.nodes[id]
	c.nodes[id] = nil
	c.mu.Unlock()
	if n == nil {
		return
	}
	c.Net.Deregister(id)
	n.Stop()
}

// Restart brings a crashed node back up from its durable store.
func (c *SimCluster) Restart(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.nodes[id] != nil {
		return // already running
	}
	c.startNode(id)
}

// Leader returns the current leader node if exactly one node claims leadership,
// else (nil, false).
func (c *SimCluster) Leader() (*raft.Node, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var leaders []*raft.Node
	for _, id := range c.ids {
		n := c.nodes[id]
		if n != nil && n.IsLeader() {
			leaders = append(leaders, n)
		}
	}
	if len(leaders) == 1 {
		return leaders[0], true
	}
	return nil, false
}

// WaitForLeader waits until a stable single leader emerges, returning it.
func (c *SimCluster) WaitForLeader(timeout time.Duration) (*raft.Node, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n, ok := c.Leader(); ok {
			// Confirm term agreement across reachable nodes for stability.
			if c.leaderStable(n) {
				return n, true
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	return nil, false
}

// leaderStable checks the leader's term matches a majority of running nodes.
func (c *SimCluster) leaderStable(leader *raft.Node) bool {
	term := leader.Status().Term
	c.mu.Lock()
	defer c.mu.Unlock()
	agree := 0
	for _, id := range c.ids {
		n := c.nodes[id]
		if n == nil {
			continue
		}
		if n.Status().Term == term {
			agree++
		}
	}
	return agree >= (len(c.ids)/2)+1
}

// Submit submits a command to the current leader, retrying on leader changes
// until success or the context expires.
func (c *SimCluster) Submit(ctx context.Context, cmd []byte) ([]byte, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, ok := c.Leader()
		if !ok {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		sctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		res, err := n.Submit(sctx, cmd)
		cancel()
		if err == nil {
			return res, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// StopAll stops every running node.
func (c *SimCluster) StopAll() {
	c.mu.Lock()
	nodes := make([]*raft.Node, 0, len(c.nodes))
	for _, id := range c.ids {
		if n := c.nodes[id]; n != nil {
			nodes = append(nodes, n)
			c.nodes[id] = nil
		}
	}
	c.mu.Unlock()
	for _, n := range nodes {
		n.Stop()
	}
}
