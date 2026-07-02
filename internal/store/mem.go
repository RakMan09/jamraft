package store

import (
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"

	raftpb "github.com/rakman09/jamraft/proto/raftpb"
)

// MemStore is an in-memory Storage implementation used by tests and the
// deterministic network simulator. It deep-copies entries so callers cannot
// mutate persisted state by accident.
type MemStore struct {
	mu   sync.Mutex
	hs   HardState
	log  map[uint64]*raftpb.LogEntry
	snap Snapshot
}

// NewMemStore returns an empty in-memory store.
func NewMemStore() *MemStore {
	return &MemStore{log: map[uint64]*raftpb.LogEntry{}}
}

func (s *MemStore) SaveHardState(hs HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hs = hs
	return nil
}

func (s *MemStore) LoadHardState() (HardState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hs, nil
}

func (s *MemStore) Append(entries []*raftpb.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range entries {
		cp := proto.Clone(e).(*raftpb.LogEntry)
		s.log[e.Index] = cp
	}
	return nil
}

func (s *MemStore) TruncateSuffix(idx uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.log {
		if k >= idx {
			delete(s.log, k)
		}
	}
	return nil
}

func (s *MemStore) TruncatePrefix(idx uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.log {
		if k <= idx {
			delete(s.log, k)
		}
	}
	return nil
}

func (s *MemStore) AllEntries() ([]*raftpb.LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	idxs := make([]uint64, 0, len(s.log))
	for k := range s.log {
		idxs = append(idxs, k)
	}
	sort.Slice(idxs, func(i, j int) bool { return idxs[i] < idxs[j] })
	out := make([]*raftpb.LogEntry, 0, len(idxs))
	for _, k := range idxs {
		out = append(out, proto.Clone(s.log[k]).(*raftpb.LogEntry))
	}
	return out, nil
}

func (s *MemStore) SaveSnapshot(snap Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(snap.Data))
	copy(cp, snap.Data)
	s.snap = Snapshot{LastIncludedIndex: snap.LastIncludedIndex, LastIncludedTerm: snap.LastIncludedTerm, Data: cp}
	return nil
}

func (s *MemStore) LoadSnapshot() (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(s.snap.Data))
	copy(cp, s.snap.Data)
	return Snapshot{LastIncludedIndex: s.snap.LastIncludedIndex, LastIncludedTerm: s.snap.LastIncludedTerm, Data: cp}, nil
}

func (s *MemStore) Close() error { return nil }
