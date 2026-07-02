package chaos

import (
	"math/rand"
	"time"

	"github.com/rakman09/jamraft/internal/cluster"
)

// runFaults injects one fault at a time (so at most a minority of nodes is ever
// affected, keeping the cluster available so operations can complete). Each
// round applies a fault, waits, then fully restores before the next round.
// Returns the number of faults injected.
func runFaults(c *cluster.SimCluster, seed int64, stop <-chan struct{}) int {
	rng := rand.New(rand.NewSource(seed ^ 0x9e3779b9))
	ids := c.IDs()
	count := 0

	sleep := func(d time.Duration) bool {
		select {
		case <-stop:
			return false
		case <-time.After(d):
			return true
		}
	}

	for {
		select {
		case <-stop:
			return count
		default:
		}

		faultDur := time.Duration(80+rng.Intn(140)) * time.Millisecond
		switch rng.Intn(4) {
		case 0: // isolate the current leader
			if leader, ok := c.Leader(); ok {
				c.Net.Isolate(leader.ID())
				count++
				if !sleep(faultDur) {
					c.Net.Rejoin(leader.ID())
					return count
				}
				c.Net.Rejoin(leader.ID())
			}
		case 1: // partition a random minority off from the majority
			minority := pickMinority(rng, ids)
			c.Net.Partition(minority)
			count++
			if !sleep(faultDur) {
				c.Net.Heal()
				return count
			}
			c.Net.Heal()
		case 2: // crash and restart a random node
			victim := ids[rng.Intn(len(ids))]
			c.Crash(victim)
			count++
			if !sleep(faultDur) {
				c.Restart(victim)
				return count
			}
			c.Restart(victim)
		case 3: // a burst of message drops
			c.Net.SetDropRate(0.15 + rng.Float64()*0.2)
			count++
			if !sleep(faultDur) {
				c.Net.SetDropRate(0)
				return count
			}
			c.Net.SetDropRate(0)
		}

		if !sleep(time.Duration(60+rng.Intn(120)) * time.Millisecond) {
			return count
		}
	}
}

// pickMinority returns a minority-sized subset of ids (at most (n-1)/2 nodes),
// so the remaining majority can still elect a leader and commit.
func pickMinority(rng *rand.Rand, ids []string) []string {
	n := len(ids)
	maxMinority := (n - 1) / 2
	if maxMinority < 1 {
		maxMinority = 1
	}
	k := 1 + rng.Intn(maxMinority)
	perm := rng.Perm(n)
	out := make([]string, 0, k)
	for i := 0; i < k; i++ {
		out = append(out, ids[perm[i]])
	}
	return out
}
