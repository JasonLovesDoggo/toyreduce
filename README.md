# ToyReduce

ToyReduce is a lightweight distributed MapReduce system written in Go.  
It implements the core ideas from Google’s MapReduce paper: **automatic parallelization, fault-tolerant task scheduling, and key-based data partitioning** — all over plain HTTP.

## Overview

The system runs three types of nodes:

- **Master:** splits the input file into chunks, tracks map/reduce task states, and assigns work to idle workers.  
- **Worker:** executes map and reduce functions, fetching tasks from the master and storing results in the cache.  
- **Cache:** acts as centralized storage for intermediate key-value pairs between map and reduce stages.

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
