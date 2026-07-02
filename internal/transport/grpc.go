package transport

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	raftpb "github.com/rakman09/jamraft/proto/raftpb"
)

// GRPCServer adapts a Handler to the generated raftpb.RaftServer interface.
type GRPCServer struct {
	raftpb.UnimplementedRaftServer
	h Handler
}

// NewGRPCServer wraps a handler (the local *raft.Node) as a gRPC service.
func NewGRPCServer(h Handler) *GRPCServer { return &GRPCServer{h: h} }

// Register registers this service on a grpc.Server.
func (s *GRPCServer) Register(gs *grpc.Server) { raftpb.RegisterRaftServer(gs, s) }

func (s *GRPCServer) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	return s.h.RequestVote(ctx, req)
}

func (s *GRPCServer) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	return s.h.AppendEntries(ctx, req)
}

func (s *GRPCServer) InstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	return s.h.InstallSnapshot(ctx, req)
}

// GRPCTransport is a raft.Transport that dials peers over gRPC lazily.
type GRPCTransport struct {
	mu    sync.Mutex
	addrs map[string]string // node id -> "host:port"
	conns map[string]*grpc.ClientConn
}

// NewGRPCTransport builds a transport given the id->address map of the cluster.
func NewGRPCTransport(addrs map[string]string) *GRPCTransport {
	return &GRPCTransport{
		addrs: addrs,
		conns: map[string]*grpc.ClientConn{},
	}
}

func (t *GRPCTransport) client(target string) (raftpb.RaftClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.conns[target]; ok {
		return raftpb.NewRaftClient(c), nil
	}
	addr, ok := t.addrs[target]
	if !ok {
		return nil, fmt.Errorf("transport: unknown peer %q", target)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	t.conns[target] = conn
	return raftpb.NewRaftClient(conn), nil
}

func (t *GRPCTransport) RequestVote(ctx context.Context, target string, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	c, err := t.client(target)
	if err != nil {
		return nil, err
	}
	return c.RequestVote(ctx, req)
}

func (t *GRPCTransport) AppendEntries(ctx context.Context, target string, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	c, err := t.client(target)
	if err != nil {
		return nil, err
	}
	return c.AppendEntries(ctx, req)
}

func (t *GRPCTransport) InstallSnapshot(ctx context.Context, target string, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	c, err := t.client(target)
	if err != nil {
		return nil, err
	}
	return c.InstallSnapshot(ctx, req)
}

// Close closes all peer connections.
func (t *GRPCTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, c := range t.conns {
		c.Close()
	}
	t.conns = map[string]*grpc.ClientConn{}
	return nil
}
