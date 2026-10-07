// Package storage keeps one newline-delimited JSON log per partition.
package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Message struct {
	Offset    int64     `json:"offset"`
	Key       string    `json:"key,omitempty"`
	Value     string    `json:"value"`
	Timestamp time.Time `json:"timestamp"`
	Partition int       `json:"partition"`
}

type Log struct {
	mu        sync.Mutex
	file      *os.File
	positions []int64 // Byte positions, not message bodies: reads stay disk-backed.
	end       int64
}

func Open(path string) (*Log, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	l := &Log{file: f}
	if err := l.recover(); err != nil {
		f.Close()
		return nil, err
	}
	return l, nil
}

func (l *Log) recover() error {
	r := bufio.NewReader(l.file)
	for {
		line, err := r.ReadBytes('\n')
		if err == io.EOF {
			// A crash may leave an incomplete final write. Keep only complete records.
			if err := l.file.Truncate(l.end); err != nil {
				return err
			}
			_, err = l.file.Seek(l.end, io.SeekStart)
			return err
		}
		if err != nil {
			return err
		}
		var m Message
		if err := json.Unmarshal(line, &m); err != nil {
			return fmt.Errorf("corrupt log at byte %d: %w", l.end, err)
		}
		if m.Offset != int64(len(l.positions)) {
			return fmt.Errorf("nonsequential offset at byte %d", l.end)
		}
		l.positions = append(l.positions, l.end)
		l.end += int64(len(line))
	}
}

func (l *Log) Append(key, value string, partition int) (Message, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	m := Message{Offset: int64(len(l.positions)), Key: key, Value: value, Timestamp: time.Now().UTC(), Partition: partition}
	b, err := json.Marshal(m)
	if err != nil {
		return m, err
	}
	b = append(b, '\n')
	n, err := l.file.WriteAt(b, l.end)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = l.file.Sync()
	} // A successful publish acknowledges an fsynced record.
	if err != nil {
		if rollback := l.file.Truncate(l.end); rollback != nil {
			return m, fmt.Errorf("append: %v; rollback: %w", err, rollback)
		}
		return m, err
	}
	l.positions = append(l.positions, l.end)
	l.end += int64(n)
	return m, nil
}

func (l *Log) Read(offset int64, limit int) ([]Message, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if offset < 0 || offset > int64(len(l.positions)) || limit < 1 || limit > 1000 {
		return nil, fmt.Errorf("invalid offset or limit")
	}
	out := make([]Message, 0)
	for i := offset; i < int64(len(l.positions)) && len(out) < limit; i++ {
		end := l.end
		if i+1 < int64(len(l.positions)) {
			end = l.positions[i+1]
		}
		b := make([]byte, end-l.positions[i])
		if _, err := l.file.ReadAt(b, l.positions[i]); err != nil {
			return nil, err
		}
		var m Message
		if err := json.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

func (l *Log) Len() int64   { l.mu.Lock(); defer l.mu.Unlock(); return int64(len(l.positions)) }
func (l *Log) Close() error { l.mu.Lock(); defer l.mu.Unlock(); return l.file.Close() }

// SaveJSON replaces a small metadata file after flushing the replacement to disk.
func SaveJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".metadata-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(tmp, path)
}
