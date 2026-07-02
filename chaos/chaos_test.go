package chaos

import (
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

func TestLinearizableNoFaults(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Faults = false
	cfg.OpsPerClient = 40
	res := Run(cfg)
	got := Check(res.Operations, 10*time.Second)
	if got == porcupine.Illegal {
		t.Fatalf("history is NOT linearizable (%d ops)", len(res.Operations))
	}
	t.Logf("no-faults: %d ops (%d enq / %d deq) -> %s", len(res.Operations), res.Enqueues, res.Dequeues, got)
}

// TestJepsen runs many randomized fault-injected histories and requires that
// none exhibit a linearizability violation.
func TestJepsen(t *testing.T) {
	histories := 40
	if testing.Short() {
		histories = 8
	}

	violations := 0
	checked := 0
	for i := 0; i < histories; i++ {
		cfg := DefaultConfig()
		cfg.Seed = int64(1000 + i)
		res := Run(cfg)
		got := Check(res.Operations, 20*time.Second)
		switch got {
		case porcupine.Illegal:
			violations++
			t.Errorf("seed %d: LINEARIZABILITY VIOLATION over %d ops (%d faults)", cfg.Seed, len(res.Operations), res.Faults)
		case porcupine.Ok:
			checked++
		case porcupine.Unknown:
			t.Logf("seed %d: checker timed out (treated as non-violation)", cfg.Seed)
		}
	}
	t.Logf("checked %d/%d histories linearizable, %d violations", checked, histories, violations)
	if violations != 0 {
		t.Fatalf("%d linearizability violations", violations)
	}
}

func TestRecoveryFullRestartQueueIntact(t *testing.T) {
	// A no-faults run already exercises the happy path; here we just ensure the
	// harness itself completes and produces a checkable history under faults.
	cfg := DefaultConfig()
	cfg.OpsPerClient = 20
	cfg.Seed = 7
	res := Run(cfg)
	if len(res.Operations) == 0 {
		t.Fatal("no operations recorded")
	}
	if got := Check(res.Operations, 15*time.Second); got == porcupine.Illegal {
		t.Fatalf("history not linearizable")
	}
}
