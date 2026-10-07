# Design notes

## Trace one message

1. `cmd/producer` posts JSON to `/messages`.
2. `internal/partition` selects a partition from the key or round-robin counter.
3. `internal/broker` computes the owner from the partition number. It calls the owner over HTTP if needed.
4. `internal/storage` serializes one JSON record, writes it at the log end, and flushes the file. The mutex gives each append a unique offset.
5. A consumer poll asks broker 0 for its assigned partitions and saved next offsets.
6. The broker reads those partitions, locally or through HTTP, and returns a batch.
7. The consumer prints the batch and commits the next offset of each partition. Broker 0 replaces the offset metadata file.

## Why these choices

HTTP keeps the protocol visible with a browser or PowerShell. JSON lines keep the storage format readable. Byte-position indexes let reads stay disk-backed without writing a separate index file. Mutexes keep concurrent access explicit. One coordinator avoids needing to implement consensus just to run a group demo.

The cost is deliberate: startup scans the full log, every message needs a disk flush, and one coordinator is a single point of availability. The system favors code that can be followed in an interview over matching Kafka's performance or availability.

## Locks and durability

Each log has its own mutex. Topic manifests and routing counters use the broker mutex. Group members, assignment versions, and commits use the coordinator mutex. Heartbeat snapshots use a separate mutex. Network calls happen outside the topic/group locks, so a slow peer does not block unrelated topic metadata changes. A commit checks the assignment version again after reads, which fences consumers when a join or expiry happens during their work.

The data directory also has an OS writer lock, released automatically on process exit. It prevents two brokers from appending to the same files. Its file can stay on disk; the lock is attached to the open handle, not the file's existence.

Logs acknowledge only after `Sync`. Small metadata updates flush a temporary file and rename it over the old file. This handles normal process restarts and avoids partially written JSON metadata. It does not promise recovery from a failed drive or every possible power-loss/filesystem scenario. Corrupt complete log records produce an error rather than being silently dropped.

## Failure behavior

| Event | Behavior |
| --- | --- |
| Consumer stops after printing but before commit | Records replay from the old offset |
| Consumer loses a partition during processing | Old-epoch commits fail; it polls again |
| Consumer process disappears | Its lease expires and remaining consumers get its partitions |
| Partition owner stops | Requests for that owner's partitions fail until it returns |
| Broker 0 stops | Consumer groups and aggregate metrics wait for the coordinator |
| HTTP response is lost after append | Producer cannot know whether it was stored; retry may duplicate it |
| Broker restarts | Topic manifests, logs, and group offsets reload; live membership starts empty |
| Topic creation reaches only some brokers | Retry the same name/count once all peers return |
| Broker count or peer order changes | Startup refuses existing data; migration is outside this project |

## What could come next

A useful next step would be a small idempotent producer API or log retention, with tests for its failure cases. Replication and leader election would be a separate, much larger project. The current design intentionally stops before those features.
