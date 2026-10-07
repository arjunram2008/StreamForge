package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/arjunram2008/StreamForge/internal/broker"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "HTTP listen address")
	data := flag.String("data", "data", "data directory (one per broker)")
	id := flag.Int("id", 0, "zero-based position in peers")
	peers := flag.String("peers", "http://127.0.0.1:8080", "ordered, comma-separated broker URLs; identical on all brokers")
	flag.Parse()
	urls := strings.Split(*peers, ",")
	seen := map[string]bool{}
	for i, s := range urls {
		s = strings.TrimRight(strings.TrimSpace(s), "/")
		u, err := url.Parse(s)
		if err != nil || u.Scheme != "http" || u.Host == "" || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil || seen[s] {
			log.Fatal("peers must be unique HTTP base URLs")
		}
		seen[s] = true
		urls[i] = s
	}
	b, err := broker.Open(broker.Config{ID: *id, DataDir: *data, Peers: urls})
	if err != nil {
		log.Fatal(err)
	}
	defer b.Close()
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	server := &http.Server{Handler: b.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	go b.Heartbeats(ctx)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	fmt.Printf("StreamForge broker-%d listening on %s (dashboard: http://%s)\n", *id, *addr, *addr)
	select {
	case <-ctx.Done():
		shutdown, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := server.Shutdown(shutdown); err != nil {
			log.Print(err)
			server.Close()
		}
	case err := <-done:
		if err != http.ErrServerClosed {
			log.Print(err)
		}
	}
}
