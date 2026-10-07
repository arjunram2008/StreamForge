package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/arjunram2008/StreamForge/internal/broker"
	"github.com/arjunram2008/StreamForge/internal/client"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	url := flag.String("broker", "http://127.0.0.1:8080", "broker URL")
	topic := flag.String("topic", "orders", "topic name")
	group := flag.String("group", "demo", "consumer group")
	name := flag.String("name", "", "unique live consumer name; random by default")
	once := flag.Bool("once", false, "consume until a poll is empty, then exit")
	flag.Parse()
	if *name == "" {
		var raw [6]byte
		if _, err := rand.Read(raw[:]); err != nil {
			return err
		}
		*name = "consumer-" + hex.EncodeToString(raw[:])
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	c := client.New(*url)
	req := broker.PollRequest{Topic: *topic, Group: *group, Consumer: *name, Limit: 100}
	defer func() { _ = c.Do("POST", "/groups/leave", req, nil) }()
	fmt.Fprintf(os.Stderr, "consumer %s in group %s; Ctrl+C stops\n", *name, *group)
	enc := json.NewEncoder(os.Stdout)
	for ctx.Err() == nil {
		var poll broker.PollResponse
		err := c.Do("POST", "/groups/poll", req, &poll)
		if err == nil {
			next := map[int]int64{}
			for _, m := range poll.Messages {
				if err := enc.Encode(m); err != nil {
					return err
				}
				next[m.Partition] = m.Offset + 1
			}
			for p, n := range next {
				err = c.Do("POST", "/groups/commit", broker.CommitRequest{Topic: *topic, Group: *group, Consumer: *name, Epoch: poll.Epoch, Partition: p, Offset: n}, nil)
				if err != nil {
					break
				}
			}
			if err == nil && *once && len(poll.Messages) == 0 {
				return nil
			}
		}
		if err != nil {
			var api *client.APIError
			if errors.As(err, &api) && api.Status >= 400 && api.Status < 500 && api.Status != 409 {
				return err
			}
			fmt.Fprintf(os.Stderr, "%v; retrying (some messages may repeat)\n", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Second):
		}
	}
	return nil
}
