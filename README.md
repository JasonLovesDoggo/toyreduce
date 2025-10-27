# ToyReduce

A tiny, hackable distributed MapReduce system written in Go.

It implements the core ideas from Google's MapReduce paper with some aspects of Hadoop's architecture: **automatic parallelization, fault-tolerant task scheduling, distributed intermediate storage, and peer-to-peer data transfer** over plain HTTP.

Workers store intermediate data locally and shuffle via P2P, eliminating the centralized storage bottleneck. Includes a real-time web UI for job monitoring and submission.



https://github.com/user-attachments/assets/c1b89e8c-bd8b-4c79-9994-19b00951a3a2



## Quick Start

```bash
# Terminal 1: Store (final results)
toyreduce store

# Terminal 2: Master (coordinator + web UI at :8080)
toyreduce master --store-url http://localhost:8081

# Terminal 3+: Workers (compute + local storage)
toyreduce worker --master-url  http://localhost:8080
toyreduce worker --master-url  http://localhost:8080

# Submit job (via CLI or web UI)
toyreduce submit --executor wordcount --path /var/data/randomfile --reduce-tasks 4
# Job submitted successfully!
#   Job ID: 550e8400-e29b-41d4-a716-446655440000
#   Status: pending

# View results
toyreduce results --job-id 550e8400-e29b-41d4-a716-446655440000
# Job Results (2 entries):
# ─────────────────────────────────────────────────────────
# foo                            532
# bar                            42
```

## How It Works

The system follows a Hadoop-like shuffle architecture:

1. **Map Phase**: Workers execute map tasks and store partitioned intermediate data in local bbolt storage
2. **Shuffle Phase**: Reduce workers fetch their assigned partition from all map workers in parallel via P2P HTTP requests
3. **Reduce Phase**: Workers execute reduce tasks and send final results to the central store for persistence

## Architecture

The system runs three types of nodes:

- **Master** (`port 8080`): Manages job queue, assigns tasks to workers, tracks completion, provides web UI
- **Workers** (ephemeral ports): Execute map/reduce tasks, store intermediate data in bbolt, serve partitions via HTTP
- **Store** (`port 8081`): Persists final job results only (not intermediate data)

Workers register with the master and provide their data endpoint. During reduce phase, workers fetch partition data directly from other workers in parallel.

## Creating Custom Executors

ToyReduce comes with built-in executors, but you can easily create your own by implementing the `Worker` interface:

```go
type Worker interface {
    Map(ctx context.Context, chunk []string, emit Emitter) error
    Reduce(ctx context.Context, key string, values []string, emit Emitter) error
    Description() string
}
```

All methods receive a `context.Context` for proper cancellation and timeout support. Check the context with `select { case <-ctx.Done(): return ctx.Err() }` to support graceful job cancellation and timeouts.

### Optional Combine Phase

For better performance with aggregation-heavy workloads, implement the optional `CombinableWorker` interface to add a combine phase:

```go
type CombinableWorker interface {
    Worker
    // Combine is called after Map to pre-aggregate values locally.
    // If not implemented, the worker's Reduce() function is used by default.
    Combine(ctx context.Context, key string, values []string, emit Emitter) error
}
```

The combine phase runs on map workers after processing their chunk, reducing intermediate data volume before shuffle. This is especially useful for aggregation operations (sum, count, average) where values for the same key can be pre-combined locally. For operations that require seeing all keys globally, implement `DisableCombinerCheck` to skip combining entirely.

**Built-in Executors:**
- [wordcount](pkg/executors/wordcount/impl.go) - Count word frequencies in text files
- [actioncount](pkg/executors/actioncount/impl.go) - Count action types in log files (format: `user_123 did login`)
- [maxvalue](pkg/executors/maxvalue/impl.go) - Find maximum value per metric key (format: `cpu_usage:75.23`)
- [urldedup](pkg/executors/urldedup/impl.go) - Deduplicate URLs (one unique URL per line)
- [average](pkg/executors/average/impl.go) - Calculate average value per metric key (format: `temperature:23.5`)

Register your executor in [pkg/workers/map.go](pkg/workers/map.go) and it will be available in the CLI and web UI.
