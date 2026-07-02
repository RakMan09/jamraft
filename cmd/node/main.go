// Command node runs a single JamRaft server: a Raft node (gRPC transport,
// bbolt storage, jukebox state machine) plus the HTTP API and party UI.
//
// Example (3-node cluster on one host):
//
//	node -id n0 -grpc :7000 -http :8080 \
//	     -peers "n0=localhost:7000,n1=localhost:7001,n2=localhost:7002" \
//	     -http-peers "n0=http://localhost:8080,n1=http://localhost:8081,n2=http://localhost:8082" \
//	     -data ./data/n0
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/rakman09/jamraft/internal/jukebox"
	"github.com/rakman09/jamraft/internal/raft"
	"github.com/rakman09/jamraft/internal/server"
	"github.com/rakman09/jamraft/internal/store"
	"github.com/rakman09/jamraft/internal/transport"
)

func parsePairs(s string) map[string]string {
	m := map[string]string{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			log.Fatalf("invalid pair %q (want id=value)", part)
		}
		m[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
	}
	return m
}

func main() {
	var (
		id         = flag.String("id", "n0", "this node's id")
		grpcAddr   = flag.String("grpc", ":7000", "gRPC listen address")
		httpAddr   = flag.String("http", ":8080", "HTTP listen address")
		peersFlag  = flag.String("peers", "", "cluster peers: id=grpcAddr,... (must include self)")
		httpPeers  = flag.String("http-peers", "", "peer HTTP base URLs: id=http://host:port,... (for leader forwarding)")
		dataDir    = flag.String("data", "", "data directory for bbolt (empty = in-memory)")
		heartbeat  = flag.Duration("heartbeat", 50*time.Millisecond, "heartbeat interval")
		electMin   = flag.Duration("election-min", 150*time.Millisecond, "min election timeout")
		electMax   = flag.Duration("election-max", 300*time.Millisecond, "max election timeout")
		snapEvery  = flag.Int("snapshot-threshold", 1000, "log entries before snapshotting")
		verbose    = flag.Bool("v", false, "verbose Raft logging")
	)
	flag.Parse()

	peers := parsePairs(*peersFlag)
	if len(peers) == 0 {
		peers[*id] = *grpcAddr // single-node fallback
	}
	if _, ok := peers[*id]; !ok {
		log.Fatalf("peers must include self (%s)", *id)
	}
	ids := make([]string, 0, len(peers))
	for pid := range peers {
		ids = append(ids, pid)
	}

	// Storage: durable bbolt, or in-memory if no data dir given.
	var st store.Storage
	if *dataDir != "" {
		if err := os.MkdirAll(*dataDir, 0o755); err != nil {
			log.Fatalf("mkdir data: %v", err)
		}
		bs, err := store.OpenBolt(filepath.Join(*dataDir, "raft.db"))
		if err != nil {
			log.Fatalf("open bolt: %v", err)
		}
		st = bs
	} else {
		st = store.NewMemStore()
	}

	logger := log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)
	rlogger := logger
	if !*verbose {
		rlogger = log.New(newLevelWriter(os.Stderr), "", log.LstdFlags)
	}

	sm := jukebox.New()
	tr := transport.NewGRPCTransport(peers)
	node, err := raft.New(raft.Config{
		ID:                 *id,
		Peers:              ids,
		Storage:            st,
		Transport:          tr,
		StateMachine:       sm,
		HeartbeatInterval:  *heartbeat,
		ElectionTimeoutMin: *electMin,
		ElectionTimeoutMax: *electMax,
		SnapshotThreshold:  *snapEvery,
		Logger:             rlogger,
	})
	if err != nil {
		log.Fatalf("new node: %v", err)
	}

	// gRPC server for inter-node RPCs.
	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("listen grpc: %v", err)
	}
	gs := grpc.NewServer()
	transport.NewGRPCServer(node).Register(gs)
	go func() {
		logger.Printf("node %s gRPC listening on %s", *id, *grpcAddr)
		if err := gs.Serve(lis); err != nil {
			logger.Printf("grpc serve: %v", err)
		}
	}()

	node.Start()

	// HTTP API + UI.
	httpBases := parsePairs(*httpPeers)
	srv := &http.Server{Addr: *httpAddr}
	shutdown := func() {
		logger.Printf("node %s shutting down", *id)
		gs.GracefulStop()
		node.Stop()
		st.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		srv.Shutdown(ctx)
		os.Exit(0)
	}
	httpSrv := server.New(node, sm, httpBases, shutdown)
	srv.Handler = httpSrv.Handler()

	go func() {
		logger.Printf("node %s HTTP listening on %s (open http://localhost%s)", *id, *httpAddr, *httpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Printf("http serve: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	shutdown()
}

// levelWriter drops verbose per-RPC Raft lines when -v is not set, keeping
// startup/leadership lines. It's a minimal filter to avoid noisy output.
type levelWriter struct{ w *os.File }

func newLevelWriter(w *os.File) *levelWriter { return &levelWriter{w: w} }

func (l *levelWriter) Write(p []byte) (int, error) {
	s := string(p)
	if strings.Contains(s, "LEADER") || strings.Contains(s, "election") ||
		strings.Contains(s, "started") || strings.Contains(s, "snapshot") ||
		strings.Contains(s, "ERROR") || strings.Contains(s, "stepping down") {
		return l.w.Write(p)
	}
	return len(p), nil
}
