package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/dustin/go-humanize"
	master2 "pkg.jsn.cam/toyreduce/internal/master"
	"pkg.jsn.cam/toyreduce/internal/store"
	"pkg.jsn.cam/toyreduce/internal/worker"
	"pkg.jsn.cam/toyreduce/pkg/executors"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "store":
		runStoreMode()
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

func runStoreMode() {
	fs := flag.NewFlagSet("store", flag.ExitOnError)
	port := fs.Int("port", 8081, "Port for store server")
	dbPath := fs.String("db-path", "var/store.db", "Path to bbolt database file")

	fs.Parse(os.Args[2:])

	log.Printf("Starting ToyReduce store node on port %d", *port)
	log.Printf("  Database: %s", *dbPath)

	server, err := store.NewServer(*dbPath)
	if err != nil {
		log.Fatalf("Failed to create store server: %v", err)
	}
	defer server.Close()

	if err := server.Start(*port); err != nil {
		log.Fatalf("Store server error: %v", err)
	}
}

func runMasterMode() {
	fs := flag.NewFlagSet("master", flag.ExitOnError)
	port := fs.Int("port", 8080, "Port for master server")
	storeURL := fs.String("store-url", "http://localhost:8081", "URL of store server")
	heartbeatTimeout := fs.Duration("heartbeat-timeout", 30*time.Second, "Worker heartbeat timeout")
	dbPath := fs.String("db-path", "var/master.db", "Path to bbolt database for persistence (empty = no persistence)")

	fs.Parse(os.Args[2:])

	log.Printf("Starting ToyReduce master node on port %d", *port)
	log.Printf("  Store URL: %s", *storeURL)
	log.Printf("  Heartbeat timeout: %v", *heartbeatTimeout)

	if *dbPath != "" {
		log.Printf("  Persistence: enabled (%s)", *dbPath)
	} else {
		log.Printf("  Persistence: disabled")
	}

	log.Printf("  Ready to accept job submissions at POST /api/jobs")

	cfg := master2.Config{
		Port:             *port,
		StoreURL:         *storeURL,
		HeartbeatTimeout: *heartbeatTimeout,
		DBPath:           *dbPath,
	}

	server, err := master2.NewServer(cfg)
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
	pollInterval := fs.Duration("poll-interval", 500*time.Millisecond, "Task polling interval")
	heartbeatInterval := fs.Duration("heartbeat-interval", 10*time.Second, "Heartbeat interval")
	ephemeralStorage := fs.Bool("ephemeral-storage", false, "Use isolated storage per worker instance (allows multiple workers on one system)")

	fs.Parse(os.Args[2:])

	log.Printf("Starting ToyReduce worker node")
	log.Printf("  Master URL: %s", *masterURL)

	cfg := worker.Config{
		MasterURL:         *masterURL,
		PollInterval:      *pollInterval,
		HeartbeatInterval: *heartbeatInterval,
		EphemeralStorage:  *ephemeralStorage,
	}

	node, err := worker.NewNode(cfg)
	if err != nil {
		log.Fatalf("Failed to create worker: %v", err)
	}

	if err := node.Start(); err != nil {
		log.Fatalf("Worker error: %v", err)
	}
}

func runSubmitJob() {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	masterURL := fs.String("master-url", "http://localhost:8080", "URL of master server")
	executor := fs.String("executor", "", "Executor to use (required)")
	path := fs.String("path", "", "Path to input file (required)")
	chunkSizeStr := fs.String("chunk-size", "16MB", "Chunk size (e.g., 16MB, 1GB, 512KB)")
	reduceTasks := fs.Int("reduce-tasks", 4, "Number of reduce tasks")

	fs.Parse(os.Args[2:])

	if *executor == "" || *path == "" {
		fmt.Println("Error: --executor and --path are required")
		fs.PrintDefaults()
		os.Exit(1)
	}

	// Parse human-readable size
	chunkBytes, err := humanize.ParseBytes(*chunkSizeStr)
	if err != nil {
		log.Fatalf("Invalid chunk size '%s': %v", *chunkSizeStr, err)
	}

	// Convert to MB (round up to nearest MB)
	chunkSizeMB := int((chunkBytes + 1024*1024 - 1) / (1024 * 1024))
	if chunkSizeMB == 0 {
		chunkSizeMB = 1 // Minimum 1MB
	}

	submitJobHTTP(*masterURL, *executor, *path, chunkSizeMB, *reduceTasks)
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

	for _, name := range executors.ListExecutors() {
		desc, err := executors.GetDescription(name)
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
  store             Start a store node
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

Store Node Options:
  --port            Port for store server (default: 8081)
  --db-path         Path to bbolt database file (default: var/store.db)

Master Node Options:
  --port            Port for master server (default: 8080)
  --store-url       URL of store server (default: http://localhost:8081)
  --heartbeat-timeout  Worker heartbeat timeout (default: 30s)

Worker Node Options:
  --master-url      URL of master server (default: http://localhost:8080)
  --poll-interval   Task polling interval (default: 2s)
  --heartbeat-interval  Heartbeat interval (default: 10s)
  --ephemeral-storage   Use isolated storage per worker instance (default: false)

Submit Job Options:
  --master-url      URL of master server (default: http://localhost:8080)
  --executor        Executor to use (required)
  --path            Path to input file (required)
  --chunk-size      Chunk size with units (default: 16MB) - e.g., 1MB, 512KB, 1GB
  --reduce-tasks    Number of reduce tasks (default: 4)

Job Management Options:
  --master-url      URL of master server (default: http://localhost:8080)
  --job-id          Job ID (required for status/results/cancel)

Example Usage:
  # Terminal 1: Start store
  toyreduce store

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
  toyreduce list-executors`)
}
