// Package transport provides two implementations of the Raft RPC transport:
//
//   - Network: an in-process, deterministic simulator that can delay, drop,
//     duplicate, and partition messages. It powers reproducible unit tests and
//     the Jepsen-style chaos harness.
//   - GRPCTransport / GRPCServer: real gRPC transport for running a cluster
//     across processes/machines.
package transport

import (
	"context"

	raftpb "github.com/rakman09/jamraft/proto/raftpb"
)

// Handler is the server side of Raft RPCs, implemented by *raft.Node.
type Handler interface {
	RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error)
	AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error)
	InstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error)
}
