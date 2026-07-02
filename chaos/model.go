// Package chaos is a Jepsen-style fault-injection harness for JamRaft. It drives
// randomized client operations against a simulated cluster while injecting
// leader crashes, partitions, message drops, and node restarts, records the
// resulting operation history, and checks it for linearizability using
// Porcupine.
package chaos

import (
	"strings"

	"github.com/anishathalye/porcupine"
)

// Operation kinds in the linearizable queue model.
const (
	KindEnqueue = "enqueue"
	KindDequeue = "dequeue" // play-next
)

// ModelInput is the input to one modeled operation.
type ModelInput struct {
	Kind  string
	Value string // enqueued value (for enqueue)
}

// encode / decode represent the queue state as a delimited string so that the
// default (==) state equality works and the model stays purely functional.
func encodeQueue(q []string) string { return strings.Join(q, "\x1f") }

func decodeQueue(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, "\x1f")
}

// QueueModel is the sequential specification of the jukebox queue:
//   - enqueue(v) appends v.
//   - dequeue() (play-next) removes and returns the front element, or "" if the
//     queue is empty.
//
// A history is linearizable iff there is some sequential ordering consistent
// with these rules and the observed (call, return) time windows.
var QueueModel = porcupine.Model{
	Init: func() interface{} { return "" },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		q := decodeQueue(state.(string))
		in := input.(ModelInput)
		out := output.(string)
		switch in.Kind {
		case KindEnqueue:
			return true, encodeQueue(append(q, in.Value))
		case KindDequeue:
			if len(q) == 0 {
				return out == "", state
			}
			if out != q[0] {
				return false, state
			}
			return true, encodeQueue(q[1:])
		default:
			return false, state
		}
	},
	Equal: func(a, b interface{}) bool { return a.(string) == b.(string) },
	DescribeOperation: func(input, output interface{}) string {
		in := input.(ModelInput)
		if in.Kind == KindEnqueue {
			return "enqueue(" + in.Value + ")"
		}
		return "play-next() -> " + output.(string)
	},
}
