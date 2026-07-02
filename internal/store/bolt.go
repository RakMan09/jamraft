package store

import (
	"encoding/binary"
	"fmt"

	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	raftpb "github.com/rakman09/jamraft/proto/raftpb"
)

var (
	bucketMeta = []byte("meta")
	bucketLog  = []byte("log")
	bucketSnap = []byte("snap")

	keyTerm     = []byte("term")
	keyVotedFor = []byte("votedFor")

	keySnapIndex = []byte("snapIndex")
	keySnapTerm  = []byte("snapTerm")
	keySnapData  = []byte("snapData")
)

// BoltStore is a bbolt-backed Storage implementation.
type BoltStore struct {
	db *bolt.DB
}

// OpenBolt opens (or creates) a bbolt database at path.
func OpenBolt(path string) (*BoltStore, error) {
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketMeta, bucketLog, bucketSnap} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &BoltStore{db: db}, nil
}

func u64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func (s *BoltStore) SaveHardState(hs HardState) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		if err := b.Put(keyTerm, u64(hs.Term)); err != nil {
			return err
		}
		return b.Put(keyVotedFor, []byte(hs.VotedFor))
	})
}

func (s *BoltStore) LoadHardState() (HardState, error) {
	var hs HardState
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		if v := b.Get(keyTerm); v != nil {
			hs.Term = binary.BigEndian.Uint64(v)
		}
		if v := b.Get(keyVotedFor); v != nil {
			hs.VotedFor = string(v)
		}
		return nil
	})
	return hs, err
}

func (s *BoltStore) Append(entries []*raftpb.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketLog)
		for _, e := range entries {
			data, err := proto.Marshal(e)
			if err != nil {
				return err
			}
			if err := b.Put(u64(e.Index), data); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BoltStore) TruncateSuffix(idx uint64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketLog)
		c := b.Cursor()
		// Iterate from idx to end, collecting keys to delete.
		var toDelete [][]byte
		for k, _ := c.Seek(u64(idx)); k != nil; k, _ = c.Next() {
			key := make([]byte, len(k))
			copy(key, k)
			toDelete = append(toDelete, key)
		}
		for _, k := range toDelete {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BoltStore) TruncatePrefix(idx uint64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketLog)
		c := b.Cursor()
		var toDelete [][]byte
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if binary.BigEndian.Uint64(k) > idx {
				break
			}
			key := make([]byte, len(k))
			copy(key, k)
			toDelete = append(toDelete, key)
		}
		for _, k := range toDelete {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BoltStore) AllEntries() ([]*raftpb.LogEntry, error) {
	var out []*raftpb.LogEntry
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketLog)
		return b.ForEach(func(k, v []byte) error {
			e := &raftpb.LogEntry{}
			if err := proto.Unmarshal(v, e); err != nil {
				return fmt.Errorf("unmarshal log entry: %w", err)
			}
			out = append(out, e)
			return nil
		})
	})
	return out, err
}

func (s *BoltStore) SaveSnapshot(snap Snapshot) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketSnap)
		if err := b.Put(keySnapIndex, u64(snap.LastIncludedIndex)); err != nil {
			return err
		}
		if err := b.Put(keySnapTerm, u64(snap.LastIncludedTerm)); err != nil {
			return err
		}
		return b.Put(keySnapData, snap.Data)
	})
}

func (s *BoltStore) LoadSnapshot() (Snapshot, error) {
	var snap Snapshot
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketSnap)
		if v := b.Get(keySnapIndex); v != nil {
			snap.LastIncludedIndex = binary.BigEndian.Uint64(v)
		}
		if v := b.Get(keySnapTerm); v != nil {
			snap.LastIncludedTerm = binary.BigEndian.Uint64(v)
		}
		if v := b.Get(keySnapData); v != nil {
			snap.Data = make([]byte, len(v))
			copy(snap.Data, v)
		}
		return nil
	})
	return snap, err
}

func (s *BoltStore) Close() error {
	return s.db.Close()
}
