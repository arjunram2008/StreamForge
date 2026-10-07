package tests

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/arjunram2008/StreamForge/internal/broker"
	"github.com/arjunram2008/StreamForge/internal/client"
	"github.com/arjunram2008/StreamForge/internal/storage"
)

func cluster(t *testing.T, n int) ([]*broker.Broker, []*httptest.Server, []broker.Config) {
	t.Helper()
	bs := make([]*broker.Broker, n)
	ss := make([]*httptest.Server, n)
	cfgs := make([]broker.Config, n)
	peers := make([]string, n)
	for i := range ss {
		ss[i] = httptest.NewUnstartedServer(nil)
		peers[i] = "http://" + ss[i].Listener.Addr().String()
	}
	for i := range bs {
		cfgs[i] = broker.Config{ID: i, DataDir: filepath.Join(t.TempDir(), "data"), Peers: peers}
		var err error
		bs[i], err = broker.Open(cfgs[i])
		if err != nil {
			t.Fatal(err)
		}
		ss[i].Config.Handler = bs[i].Handler()
		ss[i].Start()
	}
	t.Cleanup(func() {
		for _, s := range ss {
			s.Close()
		}
		for _, b := range bs {
			b.Close()
		}
	})
	return bs, ss, cfgs
}
func TestHTTPPublishConsumeOffsetsAndRestart(t *testing.T) {
	bs, ss, cfgs := cluster(t, 1)
	c := client.New(ss[0].URL)
	topic := broker.Topic{Name: "orders", Partitions: 3}
	if err := c.Do("POST", "/topics", topic, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Do("POST", "/topics", topic, nil); err != nil {
		t.Fatal("idempotent topic", err)
	}
	for i := 0; i < 6; i++ {
		var m storage.Message
		if err := c.Do("POST", "/messages", map[string]string{"topic": "orders", "value": "hello"}, &m); err != nil {
			t.Fatal(err)
		}
		if m.Partition != i%3 || m.Offset != int64(i/3) {
			t.Fatal(m)
		}
	}
	req := broker.PollRequest{Topic: "orders", Group: "test", Consumer: "alice", Limit: 100}
	var poll broker.PollResponse
	if err := c.Do("POST", "/groups/poll", req, &poll); err != nil {
		t.Fatal(err)
	}
	if len(poll.Messages) != 6 {
		t.Fatal(poll)
	}
	for _, p := range poll.Partitions {
		if err := c.Do("POST", "/groups/commit", broker.CommitRequest{Topic: "orders", Group: "test", Consumer: "alice", Epoch: poll.Epoch, Partition: p, Offset: 2}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Do("POST", "/groups/poll", req, &poll); err != nil || len(poll.Messages) != 0 {
		t.Fatalf("offsets did not advance %+v %v", poll, err)
	}
	ss[0].Close()
	if err := bs[0].Close(); err != nil {
		t.Fatal(err)
	}
	restored, err := broker.Open(cfgs[0])
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if got, err := restored.Read("orders", 0, 0, 100); err != nil || len(got) != 2 {
		t.Fatalf("messages lost: %+v %v", got, err)
	}
	replacement := httptest.NewServer(restored.Handler())
	defer replacement.Close()
	c = client.New(replacement.URL)
	if err := c.Do("POST", "/groups/poll", req, &poll); err != nil || len(poll.Messages) != 0 {
		t.Fatalf("restart offset %+v %v", poll, err)
	}
	req.Group = "replay"
	if err := c.Do("POST", "/groups/poll", req, &poll); err != nil || len(poll.Messages) != 6 {
		t.Fatalf("group replay %+v %v", poll, err)
	}
}

func TestTwoBrokerRoutingFailureAndHealth(t *testing.T) {
	bs, ss, _ := cluster(t, 2)
	c := client.New(ss[0].URL)
	if err := c.Do("POST", "/topics", broker.Topic{Name: "orders", Partitions: 4}, nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		var m storage.Message
		if err := c.Do("POST", "/messages", map[string]string{"topic": "orders", "value": "hello"}, &m); err != nil {
			t.Fatal(err)
		}
		if m.Partition != i%4 {
			t.Fatal(m)
		}
	}
	for p := 0; p < 4; p++ {
		got, err := bs[0].Read("orders", p, 0, 100)
		if err != nil || len(got) != 2 {
			t.Fatalf("partition %d: %+v %v", p, got, err)
		}
	}
	worker := client.New(ss[1].URL)
	var poll broker.PollResponse
	if err := worker.Do("POST", "/groups/poll", broker.PollRequest{Topic: "orders", Group: "g", Consumer: "a", Limit: 100}, &poll); err != nil || len(poll.Messages) != 8 {
		t.Fatalf("proxy: %+v %v", poll, err)
	}
	metrics := bs[0].Metrics()
	if !metrics.Complete || metrics.TotalMessages != 8 || len(metrics.Partitions) != 4 || metrics.Groups[0].Lag != 8 {
		t.Fatal(metrics)
	}
	ss[1].Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	bs[0].Heartbeats(ctx)
	metrics = bs[0].Metrics()
	if metrics.Complete || metrics.Brokers[1].Status != "unavailable" {
		t.Fatal("failure not visible", metrics)
	}
	if _, err := bs[0].Read("orders", 1, 0, 100); err == nil {
		t.Fatal("unavailable partition read succeeded")
	}
	if got, err := bs[0].Read("orders", 0, 0, 100); err != nil || len(got) != 2 {
		t.Fatal("healthy partition unreadable", err)
	}
	if err := c.Do("POST", "/messages", map[string]string{"topic": "orders", "value": "healthy"}, nil); err != nil {
		t.Fatal(err)
	}
	err := c.Do("POST", "/messages", map[string]string{"topic": "orders", "value": "fails"}, nil)
	var api *client.APIError
	if !errors.As(err, &api) || api.Status != 503 {
		t.Fatalf("expected visible failure: %v", err)
	}
}

func TestValidationAndTopologyGuard(t *testing.T) {
	bs, ss, cfgs := cluster(t, 1)
	c := client.New(ss[0].URL)
	for _, topic := range []broker.Topic{{Name: "../escape", Partitions: 3}, {Name: "orders", Partitions: 0}, {Name: "orders", Partitions: 65}} {
		if err := c.Do("POST", "/topics", topic, nil); err == nil {
			t.Fatal("invalid topic accepted")
		}
	}
	req, _ := http.NewRequest("POST", ss[0].URL+"/topics", strings.NewReader(`{"name":"x","partitions":1} {}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatal("trailing JSON accepted")
	}
	if other, err := broker.Open(cfgs[0]); err == nil {
		other.Close()
		t.Fatal("duplicate broker acquired data directory")
	}
	ss[0].Close()
	bs[0].Close()
	cfgs[0].Peers = []string{"http://127.0.0.1:9999"}
	if other, err := broker.Open(cfgs[0]); err == nil {
		other.Close()
		t.Fatal("changed topology accepted")
	}
}
