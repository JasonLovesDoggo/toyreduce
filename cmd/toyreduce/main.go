package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce/cache"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce/master"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce/worker"
	"pkg.jsn.cam/toyreduce/workers"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "cache":
		runCacheMode()
	case "master":
		runMasterMode()
	case "worker":
		runWorkerMode()
	case "list-executors":
		listExecutors()
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Printf("Unknown command: %s\n\n", command)
		printUsage()
		os.Exit(1)
	}
}

func runCacheMode() {
	fs := flag.NewFlagSet("cache", flag.ExitOnError)
	port := fs.Int("port", 8081, "Port for cache server")
	dbPath := fs.String("db-path", "var/cache.db", "Path to bbolt database file")

	fs.Parse(os.Args[2:])

	log.Printf("Starting ToyReduce cache node on port %d", *port)
	log.Printf("  Database: %s", *dbPath)

	server, err := cache.NewServer(*dbPath)
	if err != nil {
		log.Fatalf("Failed to create cache server: %v", err)
	}
	defer server.Close()

	if err := server.Start(*port); err != nil {
		log.Fatalf("Cache server error: %v", err)
	}
}

func runMasterMode() {
	fs := flag.NewFlagSet("master", flag.ExitOnError)
	port := fs.Int("port", 8080, "Port for master server")
	cacheURL := fs.String("cache-url", "http://localhost:8081", "URL of cache server")
	executor := fs.String("executor", "", "Executor to use (run 'toyreduce list-executors')")
	path := fs.String("path", "", "Path to input file")
	chunkSize := fs.Int("chunk-size", 1000, "Number of lines per chunk")
	reduceTasks := fs.Int("reduce-tasks", 4, "Number of reduce tasks (R partitions)")
	heartbeatTimeout := fs.Duration("heartbeat-timeout", 30*time.Second, "Worker heartbeat timeout")

	fs.Parse(os.Args[2:])

	// Validate required flags
	if *executor == "" {
		log.Fatal("--executor is required (run 'toyreduce list-executors')")
	}
	if !workers.IsValidExecutor(*executor) {
		log.Fatalf("Invalid executor: %s (run 'toyreduce list-executors')", *executor)
	}
	if *path == "" {
		log.Fatal("--path is required")
	}

	absPath, err := filepath.Abs(*path)
	if err != nil {
		log.Fatalf("Invalid path: %v", err)
	}

	// Check if file exists
	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		log.Fatalf("File does not exist: %s", absPath)
	}

	log.Printf("Starting ToyReduce master node on port %d", *port)
	log.Printf("  Cache URL: %s", *cacheURL)
	log.Printf("  Executor: %s", *executor)
	log.Printf("  Input: %s", absPath)
	log.Printf("  Chunk size: %d", *chunkSize)
	log.Printf("  Reduce tasks: %d", *reduceTasks)

	cfg := master.Config{
		Port:             *port,
		CacheURL:         *cacheURL,
		InputPath:        absPath,
		ChunkSize:        *chunkSize,
		ReduceTasks:      *reduceTasks,
		Worker:           workers.GetWorker(*executor),
		ExecutorName:     *executor,
		HeartbeatTimeout: *heartbeatTimeout,
	}

	server, err := master.NewServer(cfg)
	if err != nil {
		log.Fatalf("Failed to create master: %v", err)
	}

	if err := server.Start(*port); err != nil {
		log.Fatalf("Master server error: %v", err)
	}
}

func runWorkerMode() {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	masterURL := fs.String("master-url", "http://localhost:8080", "URL of master server")
	pollInterval := fs.Duration("poll-interval", 2*time.Second, "Task polling interval")
	heartbeatInterval := fs.Duration("heartbeat-interval", 10*time.Second, "Heartbeat interval")

	fs.Parse(os.Args[2:])

	log.Printf("Starting ToyReduce worker node")
	log.Printf("  Master URL: %s", *masterURL)

	cfg := worker.Config{
		MasterURL:         *masterURL,
		PollInterval:      *pollInterval,
		HeartbeatInterval: *heartbeatInterval,
	}

	node := worker.NewNode(cfg)
	if err := node.Start(); err != nil {
		log.Fatalf("Worker error: %v", err)
	}
}

func listExecutors() {
	fmt.Println("Available executors:")
	for _, name := range workers.ListExecutors() {
		desc, err := workers.GetDescription(name)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  %-15s %s\n", name, desc)
	}
}

func printUsage() {
	fmt.Println(`ToyReduce - A distributed MapReduce implementation

Usage:
  toyreduce <command> [options]

Commands:
  cache             Start a cache node
  master            Start a master node
  worker            Start a worker node
  list-executors    List available executors
  help              Show this help message

Cache Node Options:
  --port            Port for cache server (default: 8081)
  --db-path         Path to bbolt database file (default: var/cache.db)

Master Node Options:
  --port            Port for master server (default: 8080)
  --cache-url       URL of cache server (default: http://localhost:8081)
  --executor        Executor to use (required)
  --path            Path to input file (required)
  --chunk-size      Lines per chunk (default: 1000)
  --reduce-tasks    Number of reduce tasks/partitions (default: 4)
  --heartbeat-timeout  Worker heartbeat timeout (default: 30s)

Worker Node Options:
  --master-url      URL of master server (default: http://localhost:8080)
  --poll-interval   Task polling interval (default: 2s)
  --heartbeat-interval  Heartbeat interval (default: 10s)

Example Usage:
  # Terminal 1: Start cache
  toyreduce cache --port 8081

  # Terminal 2: Start master
  toyreduce master --cache-url http://localhost:8081 \
      --executor wordcount --path data.log --reduce-tasks 4

  # Terminal 3+: Start workers
  toyreduce worker --master-url http://localhost:8080
  toyreduce worker --master-url http://localhost:8080

  # List available executors
  toyreduce list-executors
`)
}
