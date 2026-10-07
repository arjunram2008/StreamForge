package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"

	"github.com/arjunram2008/StreamForge/internal/client"
	"github.com/arjunram2008/StreamForge/internal/storage"
)

func main() {
	url := flag.String("broker", "http://127.0.0.1:8080", "broker URL")
	topic := flag.String("topic", "orders", "topic name")
	key := flag.String("key", "", "optional message key")
	value := flag.String("message", "hello", "message value")
	flag.Parse()
	var m storage.Message
	err := client.New(*url).Do("POST", "/messages", map[string]string{"topic": *topic, "key": *key, "value": *value}, &m)
	if err != nil {
		log.Fatal(err)
	}
	json.NewEncoder(os.Stdout).Encode(m)
}
