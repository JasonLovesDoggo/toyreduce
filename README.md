# ToyReduce

ToyReduce is a lightweight distributed MapReduce system written in Go.
It implements the core ideas from Google's MapReduce paper: **automatic parallelization, fault-tolerant task scheduling, and key-based data partitioning** — all over plain HTTP.

Includes a **real-time web UI** with detailed performance metrics, job monitoring, and worker status tracking.

## Overview

The system runs three types of nodes:

- **Master:** Splits input files into chunks, manages job queue, tracks map/reduce task states, and assigns work to idle workers. Includes embedded web UI for job submission and monitoring.
- **Worker:** Executes map and reduce functions, fetching tasks from the master and storing results in the cache.
- **Cache:** Acts as centralized storage for intermediate key-value pairs between map and reduce stages.

This setup makes it easy to observe how MapReduce behaves without external systems like Hadoop or Spark.

## Run Example

```bash
# Start cache (shared intermediate storage)
toyreduce cache --port 8081

# Start master (manages job state and workers)
toyreduce master --port 8080 \
  --cache-url http://localhost:8081 \
  --executor wordcount \
  --path var/testdata.log \
  --reduce-tasks 4

# Start workers (request tasks from master)
toyreduce worker --master-url http://localhost:8080
toyreduce worker --master-url http://localhost:8080
````
