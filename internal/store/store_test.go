package store

import (
	"path/filepath"
	"testing"

	raftpb "github.com/rakman09/jamraft/proto/raftpb"
)

func entry(term, idx uint64) *raftpb.LogEntry {
	return &raftpb.LogEntry{Term: term, Index: idx, Command: []byte("cmd")}
}

func runStorageContract(t *testing.T, open func() Storage) {
	s := open()

	if err := s.SaveHardState(HardState{Term: 3, VotedFor: "node2"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Append([]*raftpb.LogEntry{entry(1, 1), entry(1, 2), entry(2, 3)}); err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Reopen and verify persistence.
	s = open()
	hs, err := s.LoadHardState()
	if err != nil {
		t.Fatal(err)
	}
	if hs.Term != 3 || hs.VotedFor != "node2" {
		t.Fatalf("hard state not persisted: %+v", hs)
	}
	entries, err := s.AllEntries()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	// TruncateSuffix from index 3 removes only entry 3.
	if err := s.TruncateSuffix(3); err != nil {
		t.Fatal(err)
	}
	entries, _ = s.AllEntries()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries after suffix truncate, got %d", len(entries))
	}

	// Snapshot round trip + prefix truncation.
	if err := s.SaveSnapshot(Snapshot{LastIncludedIndex: 2, LastIncludedTerm: 1, Data: []byte("snap")}); err != nil {
		t.Fatal(err)
	}
	if err := s.TruncatePrefix(2); err != nil {
		t.Fatal(err)
	}
	entries, _ = s.AllEntries()
	if len(entries) != 1 && len(entries) != 0 {
		t.Fatalf("unexpected entries after prefix truncate: %d", len(entries))
	}
	snap, _ := s.LoadSnapshot()
	if snap.LastIncludedIndex != 2 || string(snap.Data) != "snap" {
		t.Fatalf("snapshot not persisted: %+v", snap)
	}
	s.Close()
}

func TestBoltStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.db")
	runStorageContract(t, func() Storage {
		s, err := OpenBolt(path)
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}

func TestMemStore(t *testing.T) {
	// MemStore does not persist across "reopen", so exercise the contract on a
	// single instance instead of reopening.
	s := NewMemStore()
	if err := s.SaveHardState(HardState{Term: 3, VotedFor: "node2"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Append([]*raftpb.LogEntry{entry(1, 1), entry(1, 2), entry(2, 3)}); err != nil {
		t.Fatal(err)
	}
	hs, _ := s.LoadHardState()
	if hs.Term != 3 || hs.VotedFor != "node2" {
		t.Fatalf("hard state wrong: %+v", hs)
	}
	if err := s.TruncateSuffix(3); err != nil {
		t.Fatal(err)
	}
	if e, _ := s.AllEntries(); len(e) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(e))
	}
}
