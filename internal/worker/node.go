package worker

import (
	"fmt"
	"log"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
	"pkg.jsn.cam/toyreduce/pkg/workers"
)

// Config holds worker configuration
type Config struct {
	MasterURL         string
	PollInterval      time.Duration
	HeartbeatInterval time.Duration
	DataDir           string // Directory for worker data storage
}

// Node represents a worker node
type Node struct {
	id        string
	client    *Client
	processor *Processor
	storage   *Storage
	server    *Server
	config    Config
}

// NewNode creates a new worker node
func NewNode(cfg Config) (*Node, error) {
	workerID := uuid.New().String()
	client := NewClient(cfg.MasterURL)

	// Determine data directory
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "./data/worker"
	}

	// Create storage
	dbPath := filepath.Join(dataDir, "worker.db")
	storage, err := NewStorage(dbPath)
	if err != nil {
		return nil, fmt.Errorf("create storage: %w", err)
	}

	// Create HTTP server for serving partition data
	server, err := NewServer(storage)
	if err != nil {
		storage.Close()
		return nil, fmt.Errorf("create server: %w", err)
	}

	return &Node{
		id:      workerID,
		client:  client,
		storage: storage,
		server:  server,
		config:  cfg,
	}, nil
}

// Start starts the worker main loop
func (n *Node) Start() error {
	log.Printf("[WORKER:%s] Starting worker (version: %s)", n.id, protocol.ToyReduceVersion)

	// Start data server
	n.server.Start()
	dataEndpoint := n.server.GetEndpoint()
	log.Printf("[WORKER:%s] Data server started at %s", n.id, dataEndpoint)

	// Collect available executors
	executors := workers.ListExecutors()
	log.Printf("[WORKER:%s] Available executors: %v", n.id, executors)

	// Register with master (include data endpoint)
	regResp, err := n.client.Register(n.id, protocol.ToyReduceVersion, executors, dataEndpoint)
	if err != nil {
		log.Printf("[WORKER:%s] Registration failed: %v", n.id, err)
		return fmt.Errorf("registration failed: %w", err)
	}

	log.Printf("[WORKER:%s] Registration successful (store: %s)", n.id, regResp.StoreURL)

	// Start heartbeat goroutine
	go n.heartbeatLoop()

	// Main task processing loop
	n.taskLoop()

	return nil
}

// taskLoop polls for and processes tasks
func (n *Node) taskLoop() {
	pollInterval := n.config.PollInterval
	if pollInterval == 0 {
		pollInterval = 2 * time.Second
	}

	for {
		// Request next task
		task, err := n.client.GetNextTask(n.id)
		if err != nil {
			log.Printf("[WORKER:%s] Error getting next task: %v", n.id, err)
			time.Sleep(pollInterval)
			continue
		}

		// Process task based on type
		switch task.Type {
		case protocol.TaskTypeNone:
			// No tasks available, wait and retry
			time.Sleep(pollInterval)

		case protocol.TaskTypeMap:
			// Get worker implementation for this task's executor
			// The executor info is now embedded in the task (from current job)
			// For now, we'll instantiate processor per task when we have multi-job support
			// Currently using the old single-job approach
			if n.processor == nil {
				// Lazy initialization (for backwards compat during refactor)
				worker := getWorkerByName("wordcount") // FIXME: get from task
				if worker == nil {
					log.Printf("[WORKER:%s] No worker implementation available", n.id)
					time.Sleep(pollInterval)
					continue
				}
				n.processor = NewProcessor(worker, n.client, n.storage)
			}

			if err := n.processor.ProcessMapTask(task.MapTask, n.id); err != nil {
				log.Printf("[WORKER:%s] Map task failed: %v", n.id, err)
				// Notify master of failure
				n.client.CompleteTask(task.MapTask.ID, n.id, task.MapTask.Version, false, err.Error())
			}

		case protocol.TaskTypeReduce:
			if err := n.processor.ProcessReduceTask(task.ReduceTask, n.id); err != nil {
				log.Printf("[WORKER:%s] Reduce task failed: %v", n.id, err)
				// Notify master of failure
				n.client.CompleteTask(task.ReduceTask.ID, n.id, task.ReduceTask.Version, false, err.Error())
			}

		default:
			log.Printf("[WORKER:%s] Unknown task type: %s", n.id, task.Type)
			time.Sleep(pollInterval)
		}
	}
}

// heartbeatLoop sends periodic heartbeats to master
func (n *Node) heartbeatLoop() {
	interval := n.config.HeartbeatInterval
	if interval == 0 {
		interval = 10 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		if err := n.client.SendHeartbeat(n.id); err != nil {
			log.Printf("[WORKER:%s] Heartbeat failed: %v", n.id, err)
		}
	}
}

// getWorkerByName returns the worker implementation by name from the global registry
func getWorkerByName(name string) toyreduce.Worker {
	return workers.GetWorker(name)
}
