// Command bench measures quantifiable performance metrics for JamRaft against an
// in-process cluster (the deterministic simulator), so results are reproducible
// on a single machine. It reports:
//
//   - command throughput (enqueues/sec) at different cluster sizes,
//   - end-to-end commit latency percentiles,
//   - leader failover time (kill the leader -> service restored),
//   - linearizable read latency vs a local (non-consensus) read.
//
// Numbers reflect the consensus-pipeline overhead of this implementation on one
// host; they are not WAN latencies.
//
//	go run ./cmd/bench
package main

import (
	"context"
	"flag"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rakman09/jamraft/internal/cluster"
	"github.com/rakman09/jamraft/internal/jukebox"
)

func main() {
	var (
		clients  = flag.Int("clients", 16, "concurrent clients for throughput")
		duration = flag.Duration("duration", 3*time.Second, "throughput measurement window")
		trials   = flag.Int("failover-trials", 15, "number of leader-kill failover trials")
		reads    = flag.Int("read-samples", 500, "linearizable read latency samples")
		seed     = flag.Int64("seed", 1, "base seed")
	)
	flag.Parse()

	fmt.Println("=== JamRaft benchmarks (in-process simulator, single host) ===")
	fmt.Printf("clients=%d  window=%s  failover-trials=%d\n\n", *clients, *duration, *trials)

	for _, size := range []int{3, 5} {
		benchThroughput(size, *clients, *duration, *seed)
	}
	fmt.Println()
	benchFailover(3, *trials, *seed)
	benchFailover(5, *trials, *seed)
	fmt.Println()
	benchReadLatency(5, *reads, *seed)
}

func newCluster(size int, seed int64) (*cluster.SimCluster, []string) {
	ids := make([]string, size)
	for i := range ids {
		ids[i] = fmt.Sprintf("n%d", i)
	}
	opts := cluster.DefaultOptions()
	opts.Seed = seed
	c := cluster.New(ids, opts)
	if _, ok := c.WaitForLeader(5 * time.Second); !ok {
		panic("bench: no leader")
	}
	return c, ids
}

func benchThroughput(size, clients int, window time.Duration, seed int64) {
	c, _ := newCluster(size, seed)
	defer c.StopAll()

	var (
		ops      int64
		latMu    sync.Mutex
		latency  []time.Duration
		stop     = make(chan struct{})
		wg       sync.WaitGroup
		clientID = func(i int) string { return fmt.Sprintf("t%d", i) }
	)

	for i := 0; i < clients; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			var seq uint64
			for {
				select {
				case <-stop:
					return
				default:
				}
				seq++
				cmd := jukebox.Command{
					Op: jukebox.OpEnqueue, ClientID: clientID(i), Seq: seq,
					Song: &jukebox.Song{ID: fmt.Sprintf("%s-%d", clientID(i), seq), Title: "s"},
				}
				t0 := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				_, err := c.Submit(ctx, cmd.Encode())
				cancel()
				if err == nil {
					d := time.Since(t0)
					atomic.AddInt64(&ops, 1)
					latMu.Lock()
					latency = append(latency, d)
					latMu.Unlock()
				}
			}
		}()
	}

	start := time.Now()
	time.Sleep(window)
	close(stop)
	wg.Wait()
	elapsed := time.Since(start)

	total := atomic.LoadInt64(&ops)
	tput := float64(total) / elapsed.Seconds()
	fmt.Printf("throughput  size=%d  %8.0f enqueues/sec  (%d ops in %s)  commit latency p50=%s p99=%s\n",
		size, tput, total, elapsed.Round(time.Millisecond), pct(latency, 50), pct(latency, 99))
}

func benchFailover(size, trials int, seed int64) {
	c, ids := newCluster(size, seed)
	defer c.StopAll()

	var samples []time.Duration
	var seq uint64
	submit := func(op jukebox.Command) bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := c.Submit(ctx, op.Encode())
		return err == nil
	}

	for t := 0; t < trials; t++ {
		leader, ok := c.WaitForLeader(5 * time.Second)
		if !ok {
			continue
		}
		seq++
		submit(jukebox.Command{Op: jukebox.OpEnqueue, ClientID: "f", Seq: seq, Song: &jukebox.Song{ID: fmt.Sprintf("f-%d", seq), Title: "s"}})
		old := leader.ID()

		t0 := time.Now()
		c.Crash(old)
		// Time until a new leader commits a fresh command (service restored).
		seq++
		submit(jukebox.Command{Op: jukebox.OpEnqueue, ClientID: "f", Seq: seq, Song: &jukebox.Song{ID: fmt.Sprintf("f-%d", seq), Title: "s"}})
		samples = append(samples, time.Since(t0))

		c.Restart(old)
		_ = ids
		time.Sleep(150 * time.Millisecond) // let the restarted node settle
	}

	fmt.Printf("failover    size=%d  median=%s  p95=%s  min=%s  max=%s  (over %d kills)\n",
		size, pct(samples, 50), pct(samples, 95), minD(samples), maxD(samples), len(samples))
}

func benchReadLatency(size, n int, seed int64) {
	c, _ := newCluster(size, seed)
	defer c.StopAll()

	leader, _ := c.Leader()
	if leader == nil {
		l, _ := c.WaitForLeader(3 * time.Second)
		leader = l
	}
	// Warm the log so reads have something to confirm.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	c.Submit(ctx, jukebox.Command{Op: jukebox.OpEnqueue, ClientID: "r", Seq: 1, Song: &jukebox.Song{ID: "r-1", Title: "s"}}.Encode())
	cancel()

	var lin, local []time.Duration
	for i := 0; i < n; i++ {
		t0 := time.Now()
		rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
		leader.LinearizableRead(rctx)
		rcancel()
		lin = append(lin, time.Since(t0))

		t1 := time.Now()
		_ = c.StateMachine(leader.ID()).View() // local read, no consensus
		local = append(local, time.Since(t1))
	}
	fmt.Printf("reads       size=%d  linearizable p50=%s p99=%s   |   local(stale) p50=%s p99=%s\n",
		size, pct(lin, 50), pct(lin, 99), pct(local, 50), pct(local, 99))
}

func pct(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), d...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	idx := int(p / 100 * float64(len(s)))
	if idx >= len(s) {
		idx = len(s) - 1
	}
	return s[idx].Round(time.Microsecond)
}

func minD(d []time.Duration) time.Duration { return pct(d, 0) }
func maxD(d []time.Duration) time.Duration { return pct(d, 100) }
