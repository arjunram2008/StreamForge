# HTTP API

Base URL: `http://127.0.0.1:8080`. Bodies are JSON, capped at 1 MiB. Names contain 1–64 ASCII letters, digits, underscores, or hyphens. Errors use `{"error":"description"}`. Public endpoints have no authentication; use this locally.

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/health` | This process's status and broker ID |
| GET | `/topics` | List topic names and partition counts |
| POST | `/topics` | Create a topic; the same count is safe to retry |
| POST | `/messages` | Publish with an optional key |
| GET | `/messages?topic=orders&partition=0&offset=0&limit=100` | Read one partition without changing group offsets |
| POST | `/groups/poll` | Join/refresh a lease and read from committed offsets |
| POST | `/groups/commit` | Save the next offset for an assigned partition |
| POST | `/groups/leave` | Remove a live member |
| GET | `/metrics` | Aggregate counts, lag, broker status, and average publish rate |
| GET | `/` | Monitoring dashboard |

## Create and publish

```json
{"name":"orders","partitions":3}
```

Send this to `POST /topics`. Then send this to `POST /messages`:

```json
{"topic":"orders","key":"customer-42","value":"hello"}
```

A successful publish returns HTTP 201 with `offset`, `partition`, `timestamp`, `value`, and the optional `key`. Offset 0 is the first record in a partition. Topic creation also returns 201, including an idempotent retry. A key can be omitted or empty for round-robin selection.

## Poll and commit

`POST /groups/poll`:

```json
{"topic":"orders","group":"workers","consumer":"alice","limit":100}
```

The response contains `epoch` (the assignment version), `partitions`, `offsets`, and `messages`. The limit is **per assigned partition**, between 1 and 1000. Messages from different partitions do not have a global ordering. An empty result is immediate; clients sleep between polls. Poll more often than every 15 seconds to keep the lease.

After handling the records, send `POST /groups/commit` with the response's epoch, a partition, and its next offset. For example, after processing partition 0's record with offset 0:

```json
{"topic":"orders","group":"workers","consumer":"alice","epoch":123,"partition":0,"offset":1}
```

`123` is only a placeholder: use the actual epoch from your poll. The epoch is a Go `uint64`; clients using JavaScript should preserve large integers rather than round them. The supplied consumer CLI does this automatically. Commit each partition separately. Offsets cannot move backward or go beyond delivered records. A stale assignment gets HTTP 409: poll again and reprocess anything whose commit failed. This can cause duplicates, which is expected with at-least-once delivery.

`POST /groups/leave` uses the poll request's topic, group, and consumer fields; `limit` can be omitted. Members are otherwise removed after their lease expires.

## Failures and internal endpoints

Invalid JSON and invalid names return 400. Missing topics on public message/group requests return 404. A stale lease returns 409. Unavailable owners or coordinator return 503. Some storage or metadata errors also return 500/503. A 503 does not guarantee a publish failed before being stored: a connection can be lost after the owner appends it. Retrying can create a duplicate; there are no idempotency keys.

`/internal/topics`, `/internal/messages`, and `/internal/metrics` are for broker-to-broker calls. Internal message writes specify a partition directly; public writes always select it from the key or round-robin cursor. Internal endpoints have no authorization check, which is why the demo binds to localhost and Docker exposes host ports only on loopback.

The `/metrics` response has `complete: false` when any owner's partition counts are missing. Treat totals and lag as partial in that case. Heartbeat state includes its last probe timestamp and may lag a failure by one probe cycle plus the request timeout.
