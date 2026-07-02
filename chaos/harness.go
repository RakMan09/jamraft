package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/rakman09/jamraft/internal/cluster"
	"github.com/rakman09/jamraft/internal/jukebox"
)

// Config controls a chaos run.
type Config struct {
	Nodes        int
	Clients      int
	OpsPerClient int
	EnqueueRatio float64 // fraction of ops that are enqueues (rest are play-next)
	Seed         int64
	Faults       bool          // enable fault injection
	OpTimeout    time.Duration // max time to keep retrying a single op
}

// DefaultConfig returns a reasonable chaos configuration.
func DefaultConfig() Config {
	return Config{
		Nodes:        5,
		Clients:      3,
		OpsPerClient: 30,
		EnqueueRatio: 0.7,
		Seed:         1,
		Faults:       true,
		OpTimeout:    20 * time.Second,
	}
}

// Result summarizes a chaos run.
type Result struct {
	Operations []porcupine.Operation
	Enqueues   int
	Dequeues   int
	Faults     int
}

// Run executes one randomized history against a fresh simulated cluster and
// returns the recorded operation history plus statistics. It guarantees a clean,
// complete history: every operation is retried (idempotently, via (clientId,
// seq) de-duplication) until it definitely completes, and the network is fully
// healed before returning.
func Run(cfg Config) *Result {
	opts := cluster.DefaultOptions()
	opts.Seed = cfg.Seed
	opts.SnapshotThreshold = 40 // exercise snapshots during chaos
	ids := make([]string, cfg.Nodes)
	for i := range ids {
		ids[i] = fmt.Sprintf("n%d", i)
	}
	c := cluster.New(ids, opts)
	defer c.StopAll()

	// A little latency + reordering makes the schedule more adversarial.
	c.Net.SetLatency(1*time.Millisecond, 6*time.Millisecond)

	if _, ok := c.WaitForLeader(5 * time.Second); !ok {
		panic("chaos: no initial leader")
	}

	res := &Result{}
	var mu sync.Mutex
	record := func(op porcupine.Operation, kind string) {
		mu.Lock()
		res.Operations = append(res.Operations, op)
		if kind == KindEnqueue {
			res.Enqueues++
		} else {
			res.Dequeues++
		}
		mu.Unlock()
	}

	stopFaults := make(chan struct{})
	var faultWG sync.WaitGroup
	if cfg.Faults {
		faultWG.Add(1)
		go func() {
			defer faultWG.Done()
			res.Faults = runFaults(c, cfg.Seed, stopFaults)
		}()
	}

	start := time.Now()
	nanos := func() int64 { return time.Since(start).Nanoseconds() }

	var wg sync.WaitGroup
	for ci := 0; ci < cfg.Clients; ci++ {
		ci := ci
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(cfg.Seed*1000 + int64(ci)))
			clientID := fmt.Sprintf("c%d", ci)
			var seq uint64
			for i := 0; i < cfg.OpsPerClient; i++ {
				seq++
				if rng.Float64() < cfg.EnqueueRatio {
					value := fmt.Sprintf("%s-%d", clientID, seq)
					cmd := jukebox.Command{Op: jukebox.OpEnqueue, ClientID: clientID, Seq: seq, Song: &jukebox.Song{ID: value, Title: value}}
					call := nanos()
					mustSubmit(c, cmd, cfg.OpTimeout)
					ret := nanos()
					record(porcupine.Operation{
						ClientId: ci,
						Input:    ModelInput{Kind: KindEnqueue, Value: value},
						Output:   "",
						Call:     call,
						Return:   ret,
					}, KindEnqueue)
				} else {
					cmd := jukebox.Command{Op: jukebox.OpPlayNext, ClientID: clientID, Seq: seq}
					call := nanos()
					out := mustSubmit(c, cmd, cfg.OpTimeout)
					ret := nanos()
					var r jukebox.Result
					json.Unmarshal(out, &r)
					dequeued := ""
					if r.NowPlaying != nil {
						dequeued = r.NowPlaying.ID
					}
					record(porcupine.Operation{
						ClientId: ci,
						Input:    ModelInput{Kind: KindDequeue},
						Output:   dequeued,
						Call:     call,
						Return:   ret,
					}, KindDequeue)
				}
			}
		}()
	}
	wg.Wait()

	// Stop faults and fully heal so any final bookkeeping is unhindered.
	close(stopFaults)
	faultWG.Wait()
	c.Net.Heal()
	for _, id := range ids {
		c.Restart(id) // no-op if already running
	}
	return res
}

// mustSubmit retries a command until it completes (idempotent via dedup).
func mustSubmit(c *cluster.SimCluster, cmd jukebox.Command, timeout time.Duration) []byte {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	out, err := c.Submit(ctx, cmd.Encode())
	if err != nil {
		panic(fmt.Sprintf("chaos: op did not complete within %s: %v", timeout, err))
	}
	return out
}

// Check runs the linearizability checker over a recorded history.
func Check(ops []porcupine.Operation, timeout time.Duration) porcupine.CheckResult {
	return porcupine.CheckOperationsTimeout(QueueModel, ops, timeout)
}
