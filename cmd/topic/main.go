package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"

	"github.com/arjunram2008/StreamForge/internal/broker"
	"github.com/arjunram2008/StreamForge/internal/client"
)

func main() {
	url := flag.String("broker", "http://127.0.0.1:8080", "broker URL")
	name := flag.String("topic", "orders", "topic name")
	partitions := flag.Int("partitions", 3, "partition count")
	list := flag.Bool("list", false, "list topics instead of creating")
	flag.Parse()
	c := client.New(*url)
	var out any
	if *list {
		if err := c.Do("GET", "/topics", nil, &out); err != nil {
			log.Fatal(err)
		}
	} else {
		if err := c.Do("POST", "/topics", broker.Topic{Name: *name, Partitions: *partitions}, &out); err != nil {
			log.Fatal(err)
		}
	}
	json.NewEncoder(os.Stdout).Encode(out)
}
