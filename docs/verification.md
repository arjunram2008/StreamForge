# Verification notes

Checks performed on October 7, 2026. Local runs used Windows amd64 and Go 1.27.1.

## Local checks

- `go test ./...` passed for storage, partitioning, group coordination, and HTTP integration tests.
- `go vet ./...` passed.
- `go build ./cmd/...` passed.
- Started the compiled broker, created a three-partition `orders` topic, and used the documented producer CLI to publish two messages.
- `go run ./cmd/consumer --topic orders --group demo --name alice --once` read both messages and committed their next offsets.
- Stopped the broker, then restarted it with `go run ./cmd/broker` and the original data directory. The metrics count still included both order messages and all 1,000 benchmark messages.
- The same consumer group printed no old messages after restart. After a new publish, it read only that new message. A fresh group replayed all three order messages.
- Ran two actual broker processes with four partitions. Verified routing, group API proxying, worker-process termination, HTTP 503 for an unavailable partition, and continued reads from a healthy partition. Restarting the worker recovered its records. Restarting the coordinator preserved committed group offsets.
- Opened the dashboard in the browser and checked its topics, partition counts, health, stored offsets, and zero lag after consumption.

## CI and Docker

[GitHub Actions run for the implementation](https://github.com/arjunram2008/StreamForge/actions/runs/37688808757) passed the Windows and Linux Go checks, Linux race detector, Docker build, Compose validation, and a container publish/consume/restart/replay check.

Docker was not installed on the local Windows machine. Its runtime flow was verified on the Linux GitHub Actions runner. The optional two-broker Compose file was validated there; the two-broker runtime/recovery checks above ran as native Windows processes.

## One measured benchmark run

Command:

```powershell
go run ./cmd/benchmark --n 1000 --workers 4
```

Output:

```text
requests=1000 successful=1000 failed=0 workers=4 elapsed=459ms
messages/sec=2176.5 mean_latency=1.833ms p50=1.819ms p95=2.517ms
```

This was one local run against one broker, with three partitions, short `event-N` message values, and a file flush for each append. Latency measures the full HTTP publish request, including the append acknowledgement. It excludes topic creation and Go compilation. These numbers describe that run only; they are not a production capacity estimate or an average across repeated trials. The dashboard rate uses time since broker startup, so it is a different measurement.

Run the benchmark yourself before using a throughput number in a resume. The project and README do not promise a particular result.
