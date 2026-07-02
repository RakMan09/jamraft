// Command chaos runs many randomized fault-injected histories against a
// simulated JamRaft cluster and checks each for linearizability. It reports how
// many histories were verified and how many violations were found.
//
//	go run ./cmd/chaos -histories 500
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/rakman09/jamraft/chaos"
)

func main() {
	var (
		histories = flag.Int("histories", 500, "number of randomized histories to run")
		clients   = flag.Int("clients", 2, "concurrent clients per history")
		ops       = flag.Int("ops", 25, "operations per client")
		nodes     = flag.Int("nodes", 5, "cluster size")
		timeout   = flag.Duration("check-timeout", 10*time.Second, "linearizability check timeout per history")
		seed      = flag.Int64("seed", 1, "base random seed")
	)
	flag.Parse()

	start := time.Now()
	var (
		ok, unknown, illegal, totalOps int
	)
	for i := 0; i < *histories; i++ {
		cfg := chaos.DefaultConfig()
		cfg.Nodes = *nodes
		cfg.Clients = *clients
		cfg.OpsPerClient = *ops
		cfg.Seed = *seed + int64(i)
		res := chaos.Run(cfg)
		totalOps += len(res.Operations)
		switch chaos.Check(res.Operations, *timeout) {
		case porcupine.Ok:
			ok++
		case porcupine.Unknown:
			unknown++
		case porcupine.Illegal:
			illegal++
			fmt.Printf("!! VIOLATION at seed %d (%d ops)\n", cfg.Seed, len(res.Operations))
		}
		if (i+1)%25 == 0 {
			fmt.Printf("  %d/%d histories done (ok=%d unknown=%d illegal=%d)\n", i+1, *histories, ok, unknown, illegal)
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("\n=== JamRaft chaos report ===\n")
	fmt.Printf("histories:      %d\n", *histories)
	fmt.Printf("cluster:        %d nodes, %d clients x %d ops\n", *nodes, *clients, *ops)
	fmt.Printf("total ops:      %d\n", totalOps)
	fmt.Printf("linearizable:   %d (Ok)\n", ok)
	fmt.Printf("indeterminate:  %d (checker timeout)\n", unknown)
	fmt.Printf("VIOLATIONS:     %d\n", illegal)
	fmt.Printf("elapsed:        %s\n", elapsed.Round(time.Millisecond))

	if illegal > 0 {
		os.Exit(1)
	}
}
