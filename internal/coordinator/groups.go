// Package coordinator assigns partitions to live consumers and saves group offsets.
package coordinator

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/arjunram2008/StreamForge/internal/storage"
)

var ErrStale = errors.New("assignment changed or lease expired; poll again")

const Lease = 15 * time.Second

type Group struct {
	Topic     string                   `json:"topic"`
	Name      string                   `json:"group"`
	Offsets   map[int]int64            `json:"offsets"`
	Members   map[string]time.Time     `json:"-"`
	Epoch     uint64                   `json:"-"`
	Delivered map[string]map[int]int64 `json:"-"`
}
type Assignment struct {
	Epoch      uint64        `json:"epoch"`
	Partitions []int         `json:"partitions"`
	Offsets    map[int]int64 `json:"offsets"`
}
type View struct {
	Topic   string        `json:"topic"`
	Name    string        `json:"group"`
	Members []string      `json:"members"`
	Offsets map[int]int64 `json:"offsets"`
}
type Manager struct {
	mu     sync.Mutex
	path   string
	groups map[string]*Group
	now    func() time.Time
}

func Open(path string) (*Manager, error) {
	m := &Manager{path: path, groups: map[string]*Group{}, now: time.Now}
	b, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err == nil {
		if err := json.Unmarshal(b, &m.groups); err != nil {
			return nil, err
		}
	}
	for _, g := range m.groups {
		m.init(g)
	}
	return m, nil
}
func (m *Manager) init(g *Group) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	// Random start fences requests from a previous broker process too.
	g.Epoch = binary.LittleEndian.Uint64(b[:]) >> 1
	g.Members = map[string]time.Time{}
	g.Delivered = map[string]map[int]int64{}
	if g.Offsets == nil {
		g.Offsets = map[int]int64{}
	}
}
func (m *Manager) expire(g *Group) {
	for name, seen := range g.Members {
		if m.now().Sub(seen) > Lease {
			delete(g.Members, name)
			delete(g.Delivered, name)
			g.Epoch++
		}
	}
}
func members(g *Group) []string {
	out := make([]string, 0, len(g.Members))
	for name := range g.Members {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
func assigned(g *Group, name string, count int) []int {
	out := make([]int, 0)
	names := members(g)
	for i, n := range names {
		if n == name {
			for p := i; p < count; p += len(names) {
				out = append(out, p)
			}
			break
		}
	}
	return out
}
func contains(ps []int, p int) bool {
	for _, n := range ps {
		if n == p {
			return true
		}
	}
	return false
}

func (m *Manager) Join(topic, group, name string, count int) (Assignment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := topic + "/" + group
	g := m.groups[key]
	if g == nil {
		g = &Group{Topic: topic, Name: group}
		m.init(g)
		m.groups[key] = g
		if err := storage.SaveJSON(m.path, m.groups); err != nil {
			delete(m.groups, key)
			return Assignment{}, err
		}
	}
	m.expire(g)
	if _, ok := g.Members[name]; !ok {
		g.Epoch++
		g.Delivered[name] = map[int]int64{}
	}
	g.Members[name] = m.now()
	ps := assigned(g, name, count)
	offsets := map[int]int64{}
	for _, p := range ps {
		offsets[p] = g.Offsets[p]
	}
	return Assignment{Epoch: g.Epoch, Partitions: ps, Offsets: offsets}, nil
}

func (m *Manager) Delivered(topic, group, name string, epoch uint64, next map[int]int64, count int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.groups[topic+"/"+group]
	if g == nil {
		return ErrStale
	}
	m.expire(g)
	if g.Epoch != epoch {
		return ErrStale
	}
	ps := assigned(g, name, count)
	for p, n := range next {
		if !contains(ps, p) {
			return ErrStale
		}
		if n > g.Delivered[name][p] {
			g.Delivered[name][p] = n
		}
	}
	return nil
}

func (m *Manager) Commit(topic, group, name string, epoch uint64, p int, next int64, count int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.groups[topic+"/"+group]
	if g == nil {
		return ErrStale
	}
	m.expire(g)
	if g.Epoch != epoch || !contains(assigned(g, name, count), p) {
		return ErrStale
	}
	if next < g.Offsets[p] || next > g.Delivered[name][p] {
		return errors.New("offset must be between committed and delivered offsets")
	}
	old := g.Offsets[p]
	g.Offsets[p] = next
	if err := storage.SaveJSON(m.path, m.groups); err != nil {
		g.Offsets[p] = old
		return err
	}
	g.Members[name] = m.now()
	return nil
}

func (m *Manager) Leave(topic, group, name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if g := m.groups[topic+"/"+group]; g != nil {
		if _, ok := g.Members[name]; ok {
			delete(g.Members, name)
			delete(g.Delivered, name)
			g.Epoch++
		}
	}
}
func (m *Manager) Views() []View {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]View, 0, len(m.groups))
	for _, g := range m.groups {
		m.expire(g)
		offsets := map[int]int64{}
		for p, n := range g.Offsets {
			offsets[p] = n
		}
		out = append(out, View{Topic: g.Topic, Name: g.Name, Members: members(g), Offsets: offsets})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Topic+out[i].Name < out[j].Topic+out[j].Name })
	return out
}
