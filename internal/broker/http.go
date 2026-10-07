package broker

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/arjunram2008/StreamForge/internal/coordinator"
	"github.com/arjunram2008/StreamForge/internal/storage"
)

//go:embed web/*
var dashboard embed.FS

func sortTopics(out []Topic) {
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
}
func respond(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, status int, err error) {
	respond(w, status, map[string]string{"error": err.Error()})
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		fail(w, 400, err)
		return false
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		fail(w, 400, fmt.Errorf("expected one JSON object"))
		return false
	}
	return true
}

type PollRequest struct {
	Topic    string `json:"topic"`
	Group    string `json:"group"`
	Consumer string `json:"consumer"`
	Limit    int    `json:"limit"`
}
type PollResponse struct {
	coordinator.Assignment
	Messages []storage.Message `json:"messages"`
}
type CommitRequest struct {
	Topic     string `json:"topic"`
	Group     string `json:"group"`
	Consumer  string `json:"consumer"`
	Epoch     uint64 `json:"epoch"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

func validMember(topic, group, name string) bool {
	return ValidName(topic) && ValidName(group) && ValidName(name)
}

func (b *Broker) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		respond(w, 200, map[string]any{"status": "healthy", "broker_id": fmt.Sprintf("broker-%d", b.cfg.ID)})
	})
	mux.HandleFunc("GET /topics", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, b.Topics()) })
	mux.HandleFunc("POST /topics", func(w http.ResponseWriter, r *http.Request) {
		var t Topic
		if !decode(w, r, &t) {
			return
		}
		if !ValidName(t.Name) || t.Partitions < 1 || t.Partitions > 64 {
			fail(w, 400, fmt.Errorf("valid name and 1..64 partitions required"))
			return
		}
		if err := b.CreateTopic(t); err != nil {
			fail(w, 503, err)
			return
		}
		respond(w, 201, t)
	})
	mux.HandleFunc("POST /messages", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Topic string `json:"topic"`
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		if !decode(w, r, &req) {
			return
		}
		if !ValidName(req.Topic) {
			fail(w, 400, fmt.Errorf("invalid topic"))
			return
		}
		if _, err := b.count(req.Topic); err != nil {
			fail(w, 404, err)
			return
		}
		m, err := b.Publish(PublishRequest{Topic: req.Topic, Key: req.Key, Value: req.Value})
		if err != nil {
			fail(w, 503, err)
			return
		}
		respond(w, 201, m)
	})
	read := func(local bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			q := r.URL.Query()
			p, e1 := strconv.Atoi(q.Get("partition"))
			offset, e2 := strconv.ParseInt(q.Get("offset"), 10, 64)
			limit := 100
			var e3 error
			if q.Has("limit") {
				limit, e3 = strconv.Atoi(q.Get("limit"))
			}
			if !ValidName(q.Get("topic")) || e1 != nil || e2 != nil || e3 != nil || p < 0 || offset < 0 || limit < 1 || limit > 1000 {
				fail(w, 400, fmt.Errorf("valid topic, partition, offset and limit (1..1000) required"))
				return
			}
			n, err := b.count(q.Get("topic"))
			if err != nil {
				fail(w, 404, err)
				return
			}
			if p >= n {
				fail(w, 400, fmt.Errorf("invalid partition"))
				return
			}
			var out []storage.Message
			if local {
				out, err = b.readLocal(q.Get("topic"), p, offset, limit)
			} else {
				out, err = b.Read(q.Get("topic"), p, offset, limit)
			}
			if err != nil {
				fail(w, 503, err)
				return
			}
			respond(w, 200, out)
		}
	}
	mux.HandleFunc("GET /messages", read(false))
	mux.HandleFunc("POST /internal/topics", func(w http.ResponseWriter, r *http.Request) {
		var t Topic
		if !decode(w, r, &t) {
			return
		}
		if err := b.createLocal(t); err != nil {
			fail(w, 400, err)
			return
		}
		respond(w, 201, t)
	})
	mux.HandleFunc("POST /internal/messages", func(w http.ResponseWriter, r *http.Request) {
		var req PublishRequest
		if !decode(w, r, &req) {
			return
		}
		m, err := b.appendLocal(req)
		if err != nil {
			fail(w, 503, err)
			return
		}
		respond(w, 201, m)
	})
	mux.HandleFunc("GET /internal/messages", read(true))
	mux.HandleFunc("GET /internal/metrics", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, b.localMetrics()) })
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, b.Metrics()) })
	mux.HandleFunc("POST /groups/poll", func(w http.ResponseWriter, r *http.Request) {
		var req PollRequest
		if !decode(w, r, &req) {
			return
		}
		if !validMember(req.Topic, req.Group, req.Consumer) || req.Limit < 1 || req.Limit > 1000 {
			fail(w, 400, fmt.Errorf("valid names and limit (1..1000) required"))
			return
		}
		count, err := b.count(req.Topic)
		if err != nil {
			fail(w, 404, err)
			return
		}
		a, err := b.groups.Join(req.Topic, req.Group, req.Consumer, count)
		if err != nil {
			fail(w, 500, err)
			return
		}
		out := PollResponse{Assignment: a, Messages: make([]storage.Message, 0)}
		next := map[int]int64{}
		for _, p := range a.Partitions {
			messages, err := b.Read(req.Topic, p, a.Offsets[p], req.Limit)
			if err != nil {
				fail(w, 503, err)
				return
			}
			out.Messages = append(out.Messages, messages...)
			next[p] = a.Offsets[p]
			if len(messages) > 0 {
				next[p] = messages[len(messages)-1].Offset + 1
			}
		}
		if err := b.groups.Delivered(req.Topic, req.Group, req.Consumer, a.Epoch, next, count); err != nil {
			fail(w, 409, err)
			return
		}
		respond(w, 200, out)
	})
	mux.HandleFunc("POST /groups/commit", func(w http.ResponseWriter, r *http.Request) {
		var req CommitRequest
		if !decode(w, r, &req) {
			return
		}
		if !validMember(req.Topic, req.Group, req.Consumer) || req.Partition < 0 || req.Offset < 0 {
			fail(w, 400, fmt.Errorf("invalid commit"))
			return
		}
		count, err := b.count(req.Topic)
		if err != nil {
			fail(w, 404, err)
			return
		}
		if err := b.groups.Commit(req.Topic, req.Group, req.Consumer, req.Epoch, req.Partition, req.Offset, count); err != nil {
			status := 400
			if errors.Is(err, coordinator.ErrStale) {
				status = 409
			}
			fail(w, status, err)
			return
		}
		respond(w, 200, map[string]string{"status": "committed"})
	})
	mux.HandleFunc("POST /groups/leave", func(w http.ResponseWriter, r *http.Request) {
		var req PollRequest
		if !decode(w, r, &req) {
			return
		}
		if !validMember(req.Topic, req.Group, req.Consumer) {
			fail(w, 400, fmt.Errorf("invalid names"))
			return
		}
		b.groups.Leave(req.Topic, req.Group, req.Consumer)
		respond(w, 200, map[string]string{"status": "left"})
	})
	assets, _ := fs.Sub(dashboard, "web")
	mux.Handle("GET /", http.FileServer(http.FS(assets)))
	if b.cfg.ID == 0 {
		return mux
	}
	// Broker 0 is the single coordinator. Other brokers proxy group/metrics requests.
	target, _ := url.Parse(b.cfg.Peers[0])
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = &http.Transport{ResponseHeaderTimeout: 3 * time.Second, DialContext: (&net.Dialer{Timeout: 3 * time.Second}).DialContext}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		fail(w, 503, fmt.Errorf("coordinator unavailable: %w", err))
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/groups/") || r.URL.Path == "/metrics" {
			proxy.ServeHTTP(w, r)
			return
		}
		mux.ServeHTTP(w, r)
	})
}
