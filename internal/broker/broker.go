package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/arjunram2008/StreamForge/internal/coordinator"
	"github.com/arjunram2008/StreamForge/internal/partition"
	"github.com/arjunram2008/StreamForge/internal/storage"
)

var namePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

func ValidName(s string) bool { return namePattern.MatchString(s) }

type Topic struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
}
type Config struct {
	ID      int
	DataDir string
	Peers   []string
}
type Broker struct {
	mu        sync.RWMutex
	cfg       Config
	topics    map[string]int
	logs      map[string][]*storage.Log
	next      map[string]uint64
	groups    *coordinator.Manager
	lock      *os.File
	started   time.Time
	published uint64
	client    *http.Client
	healthMu  sync.RWMutex
	health    []Health
}
type Health struct {
	ID        int       `json:"broker_id"`
	URL       string    `json:"url"`
	Status    string    `json:"status"`
	CheckedAt time.Time `json:"checked_at"`
}

func Open(cfg Config) (*Broker, error) {
	if len(cfg.Peers) == 0 || cfg.ID < 0 || cfg.ID >= len(cfg.Peers) {
		return nil, fmt.Errorf("id must index the ordered peers list")
	}
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, err
	}
	lock, err := storage.Lock(filepath.Join(cfg.DataDir, ".broker.lock"))
	if err != nil {
		return nil, fmt.Errorf("data directory already in use: %w", err)
	}
	b := &Broker{cfg: cfg, topics: map[string]int{}, logs: map[string][]*storage.Log{}, next: map[string]uint64{}, lock: lock, started: time.Now(), client: &http.Client{Timeout: 3 * time.Second}}
	ok := false
	defer func() {
		if !ok {
			b.Close()
		}
	}()
	topologyPath := filepath.Join(cfg.DataDir, "cluster.json")
	topology := struct {
		ID    int      `json:"id"`
		Peers []string `json:"peers"`
	}{cfg.ID, cfg.Peers}
	saved, err := os.ReadFile(topologyPath)
	if err == nil {
		var previous struct {
			ID    int      `json:"id"`
			Peers []string `json:"peers"`
		}
		if err := json.Unmarshal(saved, &previous); err != nil {
			return nil, err
		}
		if previous.ID != cfg.ID || !slices.Equal(previous.Peers, cfg.Peers) {
			return nil, fmt.Errorf("broker id and ordered peers must match the original cluster.json; use a new data directory for a new topology")
		}
	} else if os.IsNotExist(err) {
		if err := storage.SaveJSON(topologyPath, topology); err != nil {
			return nil, err
		}
	} else {
		return nil, err
	}
	raw, err := os.ReadFile(filepath.Join(cfg.DataDir, "topics.json"))
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err == nil {
		if err := json.Unmarshal(raw, &b.topics); err != nil {
			return nil, err
		}
	}
	for name, n := range b.topics {
		if !ValidName(name) || n < 1 || n > 64 {
			return nil, fmt.Errorf("invalid topic metadata")
		}
		if err := b.openTopic(name, n); err != nil {
			return nil, err
		}
	}
	b.groups, err = coordinator.Open(filepath.Join(cfg.DataDir, "offsets.json"))
	if err != nil {
		return nil, err
	}
	b.health = make([]Health, len(cfg.Peers))
	for i, url := range cfg.Peers {
		b.health[i] = Health{ID: i, URL: url, Status: "unknown"}
	}
	ok = true
	return b, nil
}
func (b *Broker) openTopic(name string, n int) error {
	logs := make([]*storage.Log, n)
	for p := 0; p < n; p++ {
		if b.owner(p) == b.cfg.ID {
			l, err := storage.Open(filepath.Join(b.cfg.DataDir, name, fmt.Sprintf("partition-%d.log", p)))
			if err != nil {
				for _, l := range logs {
					if l != nil {
						l.Close()
					}
				}
				return err
			}
			logs[p] = l
		}
	}
	b.logs[name] = logs
	return nil
}
func (b *Broker) owner(p int) int { return p % len(b.cfg.Peers) }
func (b *Broker) count(topic string) (int, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	n, ok := b.topics[topic]
	if !ok {
		return 0, fmt.Errorf("topic does not exist")
	}
	return n, nil
}
func (b *Broker) createLocal(t Topic) error {
	if !ValidName(t.Name) || t.Partitions < 1 || t.Partitions > 64 {
		return fmt.Errorf("topic needs a valid name and 1..64 partitions")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if n, ok := b.topics[t.Name]; ok {
		if n != t.Partitions {
			return fmt.Errorf("topic already exists with %d partitions", n)
		}
		return nil
	}
	if err := b.openTopic(t.Name, t.Partitions); err != nil {
		return err
	}
	b.topics[t.Name] = t.Partitions
	if err := storage.SaveJSON(filepath.Join(b.cfg.DataDir, "topics.json"), b.topics); err != nil {
		delete(b.topics, t.Name)
		for _, l := range b.logs[t.Name] {
			if l != nil {
				l.Close()
			}
		}
		delete(b.logs, t.Name)
		return err
	}
	return nil
}
func (b *Broker) CreateTopic(t Topic) error {
	if err := b.createLocal(t); err != nil {
		return err
	}
	// Idempotent: retry the same creation if a peer was unavailable halfway through.
	for id := range b.cfg.Peers {
		if id != b.cfg.ID {
			if err := b.remote(id, "POST", "/internal/topics", t, nil); err != nil {
				return err
			}
		}
	}
	return nil
}
func (b *Broker) Topics() []Topic {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]Topic, 0, len(b.topics))
	for name, n := range b.topics {
		out = append(out, Topic{name, n})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

type PublishRequest struct {
	Topic     string `json:"topic"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	Partition int    `json:"partition"`
}

func (b *Broker) Publish(req PublishRequest) (storage.Message, error) {
	b.mu.Lock()
	n, ok := b.topics[req.Topic]
	if !ok {
		b.mu.Unlock()
		return storage.Message{}, fmt.Errorf("topic does not exist")
	}
	next := b.next[req.Topic]
	req.Partition = partition.Select(req.Key, n, &next)
	b.next[req.Topic] = next
	b.mu.Unlock()
	if id := b.owner(req.Partition); id != b.cfg.ID {
		var m storage.Message
		err := b.remote(id, "POST", "/internal/messages", req, &m)
		return m, err
	}
	return b.appendLocal(req)
}
func (b *Broker) appendLocal(req PublishRequest) (storage.Message, error) {
	b.mu.RLock()
	logs := b.logs[req.Topic]
	if req.Partition < 0 || req.Partition >= len(logs) || logs[req.Partition] == nil {
		b.mu.RUnlock()
		return storage.Message{}, fmt.Errorf("partition not owned by this broker")
	}
	l := logs[req.Partition]
	b.mu.RUnlock()
	m, err := l.Append(req.Key, req.Value, req.Partition)
	if err == nil {
		b.mu.Lock()
		b.published++
		b.mu.Unlock()
	}
	return m, err
}
func (b *Broker) Read(topic string, p int, offset int64, limit int) ([]storage.Message, error) {
	n, err := b.count(topic)
	if err != nil {
		return nil, err
	}
	if p < 0 || p >= n {
		return nil, fmt.Errorf("invalid partition")
	}
	if id := b.owner(p); id != b.cfg.ID {
		var out []storage.Message
		err := b.remote(id, "GET", fmt.Sprintf("/internal/messages?topic=%s&partition=%d&offset=%d&limit=%d", topic, p, offset, limit), nil, &out)
		return out, err
	}
	return b.readLocal(topic, p, offset, limit)
}
func (b *Broker) readLocal(topic string, p int, offset int64, limit int) ([]storage.Message, error) {
	b.mu.RLock()
	logs := b.logs[topic]
	if p < 0 || p >= len(logs) || logs[p] == nil {
		b.mu.RUnlock()
		return nil, fmt.Errorf("partition not owned by this broker")
	}
	l := logs[p]
	b.mu.RUnlock()
	return l.Read(offset, limit)
}
func (b *Broker) remote(id int, method, path string, input, output any) error {
	var body io.Reader
	if input != nil {
		raw, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, b.cfg.Peers[id]+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return fmt.Errorf("broker-%d unavailable: %w", id, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("broker-%d returned %d: %s", id, resp.StatusCode, raw)
	}
	if output != nil {
		return json.NewDecoder(resp.Body).Decode(output)
	}
	return nil
}
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var first error
	for _, logs := range b.logs {
		for _, l := range logs {
			if l != nil {
				if err := l.Close(); err != nil && first == nil {
					first = err
				}
			}
		}
	}
	if b.lock != nil {
		if err := b.lock.Close(); err != nil && first == nil {
			first = err
		}
		b.lock = nil
	}
	return first
}
