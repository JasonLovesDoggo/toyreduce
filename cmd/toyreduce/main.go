package main

import (
	"flag"
	"fmt"
	"log"
	"os"
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
	case "submit":
		runSubmitJob()
	case "jobs":
		runListJobs()
	case "status":
		runJobStatus()
	case "results":
		runJobResults()
	case "cancel":
		runCancelJob()
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
	heartbeatTimeout := fs.Duration("heartbeat-timeout", 30*time.Second, "Worker heartbeat timeout")
	dbPath := fs.String("db-path", "var/master.db", "Path to bbolt database for persistence (empty = no persistence)")

	fs.Parse(os.Args[2:])

	log.Printf("Starting ToyReduce master node on port %d", *port)
	log.Printf("  Cache URL: %s", *cacheURL)
	log.Printf("  Heartbeat timeout: %v", *heartbeatTimeout)
	if *dbPath != "" {
		log.Printf("  Persistence: enabled (%s)", *dbPath)
	} else {
		log.Printf("  Persistence: disabled")
	}
	log.Printf("  Ready to accept job submissions at POST /api/jobs")

	cfg := master.Config{
		Port:             *port,
		CacheURL:         *cacheURL,
		HeartbeatTimeout: *heartbeatTimeout,
		DBPath:           *dbPath,
	}

	server, err := master.NewServer(cfg)
	if err != nil {
		log.Fatalf("Failed to create master: %v", err)
	}
	defer server.Close()

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

func runSubmitJob() {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	masterURL := fs.String("master-url", "http://localhost:8080", "URL of master server")
	executor := fs.String("executor", "", "Executor to use (required)")
	path := fs.String("path", "", "Path to input file (required)")
	chunkSize := fs.Int("chunk-size", 1000, "Lines per chunk")
	reduceTasks := fs.Int("reduce-tasks", 4, "Number of reduce tasks")

	fs.Parse(os.Args[2:])

	if *executor == "" || *path == "" {
		fmt.Println("Error: --executor and --path are required")
		fs.PrintDefaults()
		os.Exit(1)
	}

	submitJobHTTP(*masterURL, *executor, *path, *chunkSize, *reduceTasks)
}

func runListJobs() {
	fs := flag.NewFlagSet("jobs", flag.ExitOnError)
	masterURL := fs.String("master-url", "http://localhost:8080", "URL of master server")
	fs.Parse(os.Args[2:])

	listJobsHTTP(*masterURL)
}

func runJobStatus() {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	masterURL := fs.String("master-url", "http://localhost:8080", "URL of master server")
	jobID := fs.String("job-id", "", "Job ID (required)")
	fs.Parse(os.Args[2:])

	if *jobID == "" {
		fmt.Println("Error: --job-id is required")
		fs.PrintDefaults()
		os.Exit(1)
	}

	getJobStatusHTTP(*masterURL, *jobID)
}

func runJobResults() {
	fs := flag.NewFlagSet("results", flag.ExitOnError)
	masterURL := fs.String("master-url", "http://localhost:8080", "URL of master server")
	jobID := fs.String("job-id", "", "Job ID (required)")
	fs.Parse(os.Args[2:])

	if *jobID == "" {
		fmt.Println("Error: --job-id is required")
		fs.PrintDefaults()
		os.Exit(1)
	}

	getJobResultsHTTP(*masterURL, *jobID)
}

func runCancelJob() {
	fs := flag.NewFlagSet("cancel", flag.ExitOnError)
	masterURL := fs.String("master-url", "http://localhost:8080", "URL of master server")
	jobID := fs.String("job-id", "", "Job ID (required)")
	fs.Parse(os.Args[2:])

	if *jobID == "" {
		fmt.Println("Error: --job-id is required")
		fs.PrintDefaults()
		os.Exit(1)
	}

	cancelJobHTTP(*masterURL, *jobID)
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

Server Commands (long-running processes):
  cache             Start a cache node
  master            Start a master node
  worker            Start a worker node

Client Commands (job management):
  submit            Submit a new MapReduce job
  jobs              List all jobs
  status            Get status of a specific job
  results           Get results of a completed job
  cancel            Cancel a queued or running job

Utility Commands:
  list-executors    List available executors
  help              Show this help message

Cache Node Options:
  --port            Port for cache server (default: 8081)
  --db-path         Path to bbolt database file (default: var/cache.db)

Master Node Options:
  --port            Port for master server (default: 8080)
  --cache-url       URL of cache server (default: http://localhost:8081)
  --heartbeat-timeout  Worker heartbeat timeout (default: 30s)

Worker Node Options:
  --master-url      URL of master server (default: http://localhost:8080)
  --poll-interval   Task polling interval (default: 2s)
  --heartbeat-interval  Heartbeat interval (default: 10s)

Submit Job Options:
  --master-url      URL of master server (default: http://localhost:8080)
  --executor        Executor to use (required)
  --path            Path to input file (required)
  --chunk-size      Lines per chunk (default: 1000)
  --reduce-tasks    Number of reduce tasks (default: 4)

Job Management Options:
  --master-url      URL of master server (default: http://localhost:8080)
  --job-id          Job ID (required for status/results/cancel)

Example Usage:
  # Terminal 1: Start cache
  toyreduce cache

  # Terminal 2: Start master
  toyreduce master

  # Terminal 3+: Start workers
  toyreduce worker
  toyreduce worker

  # Submit a job
  toyreduce submit --executor wordcount --path data.log --reduce-tasks 4

  # List all jobs
  toyreduce jobs

  # Check job status
  toyreduce status --job-id <job-id>

  # Get results when complete
  toyreduce results --job-id <job-id>

  # Cancel a job
  toyreduce cancel --job-id <job-id>

  # List available executors
  toyreduce list-executors
`)
}
