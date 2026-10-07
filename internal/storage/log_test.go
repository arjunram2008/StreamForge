package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestPersistenceAndPartialTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "orders", "partition-0.log")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"hello\nworld", "日本語", ""} {
		if _, err := l.Append("key", v, 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(`{"offset":3`)
	f.Close()
	l, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	got, err := l.Read(1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if l.Len() != 3 || len(got) != 2 || got[0].Value != "日本語" || got[0].Offset != 1 {
		t.Fatalf("bad restored messages: %+v", got)
	}
	m, err := l.Append("", "after crash", 0)
	if err != nil || m.Offset != 3 {
		t.Fatalf("append after recovery: %+v %v", m, err)
	}
	if _, err := l.Read(-1, 10); err == nil {
		t.Fatal("negative offset accepted")
	}
	if _, err := l.Read(5, 10); err == nil {
		t.Fatal("offset beyond log accepted")
	}
}
func TestCompletedCorruptRecordFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.log")
	os.WriteFile(path, []byte("not JSON\n"), 0644)
	if l, err := Open(path); err == nil {
		l.Close()
		t.Fatal("completed corrupt record was discarded")
	}
}
func TestConcurrentAppendAndRead(t *testing.T) {
	l, err := Open(filepath.Join(t.TempDir(), "p.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				if _, err := l.Append("", fmt.Sprintf("%d/%d", w, i), 0); err != nil {
					t.Error(err)
				}
				if _, err := l.Read(0, 100); err != nil {
					t.Error(err)
				}
			}
		}(w)
	}
	wg.Wait()
	got, err := l.Read(0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 40 {
		t.Fatalf("got %d records", len(got))
	}
	seen := map[string]bool{}
	for i, m := range got {
		if m.Offset != int64(i) || seen[m.Value] {
			t.Fatalf("nonunique append: %+v", m)
		}
		seen[m.Value] = true
	}
}
func TestExclusiveDirectoryLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".lock")
	first, err := Lock(path)
	if err != nil {
		t.Fatal(err)
	}
	if second, err := Lock(path); err == nil {
		second.Close()
		t.Fatal("second writer acquired lock")
	}
	first.Close()
	third, err := Lock(path)
	if err != nil {
		t.Fatal(err)
	}
	third.Close()
}
