package coordinator

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestAssignmentsFencingAndResume(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offsets.json")
	m, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	a, err := m.Join("orders", "workers", "alice", 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Partitions) != 4 {
		t.Fatal(a)
	}
	if err := m.Delivered("orders", "workers", "alice", a.Epoch, map[int]int64{0: 3}, 4); err != nil {
		t.Fatal(err)
	}
	bob, err := m.Join("orders", "workers", "bob", 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Commit("orders", "workers", "alice", a.Epoch, 0, 3, 4); !errors.Is(err, ErrStale) {
		t.Fatalf("stale commit: %v", err)
	}
	a, _ = m.Join("orders", "workers", "alice", 4)
	if len(a.Partitions) != 2 || a.Partitions[0] != 0 || a.Partitions[1] != 2 || bob.Partitions[0] != 1 || bob.Partitions[1] != 3 {
		t.Fatalf("bad assignments: %+v %+v", a, bob)
	}
	if err := m.Delivered("orders", "workers", "alice", a.Epoch, map[int]int64{0: 3}, 4); err != nil {
		t.Fatal(err)
	}
	if err := m.Commit("orders", "workers", "alice", a.Epoch, 0, 4, 4); err == nil {
		t.Fatal("committed beyond delivered records")
	}
	if err := m.Commit("orders", "workers", "alice", a.Epoch, 1, 1, 4); !errors.Is(err, ErrStale) {
		t.Fatal("committed unowned partition")
	}
	if err := m.Commit("orders", "workers", "alice", a.Epoch, 0, 3, 4); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	resume, err := restored.Join("orders", "workers", "alice", 4)
	if err != nil {
		t.Fatal(err)
	}
	if resume.Offsets[0] != 3 || len(resume.Partitions) != 4 {
		t.Fatal(resume)
	}
	if err := restored.Commit("orders", "workers", "alice", a.Epoch, 0, 3, 4); !errors.Is(err, ErrStale) {
		t.Fatal("old process commit accepted")
	}
	independent, _ := restored.Join("orders", "other-group", "alice", 4)
	if independent.Offsets[0] != 0 {
		t.Fatal("groups share offsets")
	}
}
func TestLeaseExpiryAndLeave(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "offsets.json"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	m.now = func() time.Time { return now }
	alice, _ := m.Join("t", "g", "alice", 4)
	m.Join("t", "g", "bob", 4)
	now = now.Add(Lease + time.Second)
	alice, _ = m.Join("t", "g", "alice", 4)
	if len(alice.Partitions) != 4 || len(m.Views()[0].Members) != 1 {
		t.Fatal("expired consumer still owns partitions")
	}
	m.Join("t", "g", "bob", 4)
	m.Leave("t", "g", "bob")
	alice, _ = m.Join("t", "g", "alice", 4)
	if len(alice.Partitions) != 4 {
		t.Fatal("leave did not reassign")
	}
	now = now.Add(Lease + time.Second)
	if err := m.Commit("t", "g", "alice", alice.Epoch, 0, 0, 4); !errors.Is(err, ErrStale) {
		t.Fatal("expired lease committed")
	}
}
