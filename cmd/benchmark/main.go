package main

import (
	"flag"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/arjunram2008/StreamForge/internal/broker"
	"github.com/arjunram2008/StreamForge/internal/client"
)

func main() {
	url := flag.String("broker", "http://127.0.0.1:8080", "broker URL")
	topic := flag.String("topic", "benchmark", "dedicated topic")
	n := flag.Int("n", 1000, "number of publish requests")
	workers := flag.Int("workers", 4, "concurrent producers")
	flag.Parse()
	if *n < 1 || *workers < 1 || *workers > 256 {
		log.Fatal("n must be positive; workers must be 1..256")
	}
	c := client.New(*url)
	if err := c.Do("POST", "/topics", broker.Topic{Name: *topic, Partitions: 3}, nil); err != nil {
		log.Fatal(err)
	}
	jobs := make(chan int)
	latencies := make(chan time.Duration, *n)
	failures := make(chan error, *n)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				at := time.Now()
				err := c.Do("POST", "/messages", map[string]string{"topic": *topic, "value": fmt.Sprintf("event-%d", i)}, nil)
				if err != nil {
					failures <- err
				} else {
					latencies <- time.Since(at)
				}
			}
		}()
	}
	for i := 0; i < *n; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	elapsed := time.Since(start)
	close(latencies)
	close(failures)
	samples := make([]time.Duration, 0, *n)
	var total time.Duration
	for d := range latencies {
		samples = append(samples, d)
		total += d
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	fmt.Printf("requests=%d successful=%d failed=%d workers=%d elapsed=%s\n", *n, len(samples), len(failures), *workers, elapsed.Round(time.Millisecond))
	if len(samples) > 0 {
		fmt.Printf("messages/sec=%.1f mean_latency=%s p50=%s p95=%s\n", float64(len(samples))/elapsed.Seconds(), (total / time.Duration(len(samples))).Round(time.Microsecond), samples[(len(samples)-1)*50/100].Round(time.Microsecond), samples[(len(samples)-1)*95/100].Round(time.Microsecond))
	}
	if err, ok := <-failures; ok {
		log.Fatalf("benchmark had failures; first error: %v", err)
	}
}
