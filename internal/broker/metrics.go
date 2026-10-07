package broker

import (
	"context"
	"time"

	"github.com/arjunram2008/StreamForge/internal/coordinator"
)

type PartitionMetric struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	BrokerID  int    `json:"broker_id"`
	Messages  int64  `json:"messages"`
}
type LocalMetrics struct {
	Partitions []PartitionMetric `json:"partitions"`
	Published  uint64            `json:"published_since_start"`
	Uptime     float64           `json:"uptime_seconds"`
}
type GroupMetric struct {
	coordinator.View
	Lag int64 `json:"lag"`
}
type Metrics struct {
	TotalMessages     int64             `json:"total_messages"`
	ActiveTopics      int               `json:"active_topics"`
	MessagesPerSecond float64           `json:"messages_per_second"`
	Uptime            float64           `json:"uptime_seconds"`
	Complete          bool              `json:"complete"`
	Topics            []Topic           `json:"topics"`
	Partitions        []PartitionMetric `json:"partitions"`
	Groups            []GroupMetric     `json:"groups"`
	Brokers           []Health          `json:"brokers"`
}

func (b *Broker) localMetrics() LocalMetrics {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := LocalMetrics{Partitions: make([]PartitionMetric, 0), Published: b.published, Uptime: time.Since(b.started).Seconds()}
	for _, t := range sortedTopics(b.topics) {
		for p, l := range b.logs[t.Name] {
			if l != nil {
				out.Partitions = append(out.Partitions, PartitionMetric{Topic: t.Name, Partition: p, BrokerID: b.cfg.ID, Messages: l.Len()})
			}
		}
	}
	return out
}
func sortedTopics(topics map[string]int) []Topic {
	// Caller already holds the broker lock.
	out := make([]Topic, 0, len(topics))
	for name, n := range topics {
		out = append(out, Topic{name, n})
	}
	sortTopics(out)
	return out
}
func (b *Broker) Metrics() Metrics {
	b.healthMu.RLock()
	health := append([]Health(nil), b.health...)
	b.healthMu.RUnlock()
	out := Metrics{Uptime: time.Since(b.started).Seconds(), Complete: true, Topics: b.Topics(), Brokers: health, Partitions: make([]PartitionMetric, 0), Groups: make([]GroupMetric, 0)}
	out.ActiveTopics = len(out.Topics)
	for id := range b.cfg.Peers {
		var local LocalMetrics
		if id == b.cfg.ID {
			local = b.localMetrics()
		} else if err := b.remote(id, "GET", "/internal/metrics", nil, &local); err != nil {
			out.Complete = false
			out.Brokers[id].Status = "unavailable"
			continue
		}
		out.Partitions = append(out.Partitions, local.Partitions...)
		if local.Uptime > 0 {
			out.MessagesPerSecond += float64(local.Published) / local.Uptime
		}
	}
	ends := map[string]map[int]int64{}
	for _, p := range out.Partitions {
		out.TotalMessages += p.Messages
		if ends[p.Topic] == nil {
			ends[p.Topic] = map[int]int64{}
		}
		ends[p.Topic][p.Partition] = p.Messages
	}
	for _, g := range b.groups.Views() {
		metric := GroupMetric{View: g}
		for p, end := range ends[g.Topic] {
			if end > g.Offsets[p] {
				metric.Lag += end - g.Offsets[p]
			}
		}
		out.Groups = append(out.Groups, metric)
	}
	return out
}

// Check peers every two seconds. Health probes do not elect leaders or move data.
func (b *Broker) Heartbeats(ctx context.Context) {
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		states := make([]Health, len(b.cfg.Peers))
		for id, url := range b.cfg.Peers {
			status := "healthy"
			if id != b.cfg.ID {
				if err := b.remote(id, "GET", "/health", nil, nil); err != nil {
					status = "unavailable"
				}
			}
			states[id] = Health{ID: id, URL: url, Status: status, CheckedAt: time.Now().UTC()}
		}
		b.healthMu.Lock()
		b.health = states
		b.healthMu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
