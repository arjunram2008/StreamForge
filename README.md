# StreamForge

StreamForge is a small distributed event-streaming system written in Go. It supports topics, partitions, persistent messages, producers, consumers, and consumer offsets. It is a project for learning how systems like Kafka organize events, with enough code to follow from an HTTP request down to a log file.

- Publish keyed or unkeyed messages to partitioned topics.
- Keep messages and group offsets after a broker restart.
- Share partitions between consumers in the same group.
- Run one broker or a small cluster with fixed partition ownership.
- Inspect broker health, throughput, partition counts, and consumer lag in a plain HTML/CSS/JavaScript dashboard.

The backend uses only Go's standard library. No database, UI build step, or Docker installation is required for local use.

```mermaid
flowchart LR
    P[Producer CLI] --> H[Go HTTP broker]
    H --> R{Partition owner}
    R --> L0[Partition 0 log]
    R --> L1[Partition 1 log]
    R --> L2[Partition 2 log]
    L0 --> C[Consumer group]
    L1 --> C
    L2 --> C
    C --> O[Saved next offsets]
    D[Dashboard] --> H
```

