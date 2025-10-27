package worker

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"pkg.jsn.cam/toyreduce/pkg/executors"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
)

// Config holds worker configuration
type Config struct {
	MasterURL         string
	PollInterval      time.Duration
	HeartbeatInterval time.Duration
	DataDir           string // Directory for worker data storage
	EphemeralStorage  bool   // Use unique database path per worker instance (default: true)
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
		dataDir = "./var/worker"
	}

	// Determine database path based on ephemeral storage flag
	var dbPath string
	if cfg.EphemeralStorage {
		// Use UUID-based path - each worker instance gets isolated database
		dbPath = filepath.Join(dataDir, workerID, "worker.db")
	} else {
		// Use shared path - single worker with persistent storage across restarts
		dbPath = filepath.Join(dataDir, "worker.db")
	}

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
func (n *Node) Start(ctx context.Context) error {
	log.Printf("[WORKER:%s] Starting worker (version: %s)", n.id, protocol.ToyReduceVersion)

	// Start data server
	n.server.Start()
	dataEndpoint := n.server.GetEndpoint()
	log.Printf("[WORKER:%s] Data server started at %s", n.id, dataEndpoint)

	// Collect available executors
	executors := executors.ListExecutors()
	log.Printf("[WORKER:%s] Available executors: %v", n.id, executors)

	// Register with master (include data endpoint)
	regResp, err := n.client.Register(ctx, n.id, protocol.ToyReduceVersion, executors, dataEndpoint)
	if err != nil {
		log.Printf("[WORKER:%s] Registration failed: %v", n.id, err)
		return fmt.Errorf("registration failed: %w", err)
	}

	log.Printf("[WORKER:%s] Registration successful (store: %s)", n.id, regResp.StoreURL)

	// Start heartbeat goroutine
	go n.heartbeatLoop(ctx)

	// Main task processing loop
	n.taskLoop(ctx)

	return nil
}

// taskLoop polls for and processes tasks
func (n *Node) taskLoop(ctx context.Context) {
	pollInterval := n.config.PollInterval
	if pollInterval == 0 {
		pollInterval = 500 * time.Millisecond
	}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[WORKER:%s] Task loop shutting down", n.id)
			return
		case <-ticker.C:
			// Request next task
			task, err := n.client.GetNextTask(ctx, n.id)
			if err != nil {
				log.Printf("[WORKER:%s] Error getting next task: %v", n.id, err)
				continue
			}

			// Process task based on type
			switch task.Type {
			case protocol.TaskTypeNone:
				// No tasks available, wait and retry
				continue

			case protocol.TaskTypeMap:
				// Get worker implementation from task's executor field
				worker := executors.GetExecutor(task.MapTask.Executor)
				if worker == nil {
					log.Printf("[WORKER:%s] Unknown executor: %s", n.id, task.MapTask.Executor)
					continue
				}

				// Create processor for this executor (supports different executors per task)
				if n.processor == nil || n.processor.worker != worker {
					n.processor = NewProcessor(worker, n.client, n.storage)
				}

				if err := n.processor.ProcessMapTask(ctx, task.MapTask, n.id); err != nil {
					log.Printf("[WORKER:%s] Map task failed: %v", n.id, err)
					// Notify master of failure
					n.client.CompleteTask(ctx, task.MapTask.ID, n.id, task.MapTask.Version, false, err.Error())
				}

			case protocol.TaskTypeReduce:
				// Get worker implementation from task's executor field
				worker := executors.GetExecutor(task.ReduceTask.Executor)
				if worker == nil {
					log.Printf("[WORKER:%s] Unknown executor: %s", n.id, task.ReduceTask.Executor)
					continue
				}

				// Create processor for this executor (supports different executors per task)
				if n.processor == nil || n.processor.worker != worker {
					n.processor = NewProcessor(worker, n.client, n.storage)
				}

				if err := n.processor.ProcessReduceTask(ctx, task.ReduceTask, n.id); err != nil {
					log.Printf("[WORKER:%s] Reduce task failed: %v", n.id, err)
					// Notify master of failure
					n.client.CompleteTask(ctx, task.ReduceTask.ID, n.id, task.ReduceTask.Version, false, err.Error())
				}

			default:
				log.Printf("[WORKER:%s] Unknown task type: %s", n.id, task.Type)
			}
		}
	}
}

// heartbeatLoop sends periodic heartbeats to master
func (n *Node) heartbeatLoop(ctx context.Context) {
	interval := n.config.HeartbeatInterval
	if interval == 0 {
		interval = 10 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[WORKER:%s] Heartbeat loop shutting down", n.id)
			return
		case <-ticker.C:
			ok, err := n.client.SendHeartbeat(ctx, n.id)
			if err != nil {
				log.Printf("[WORKER:%s] Heartbeat request failed: %v", n.id, err)
				continue
			}

			// If heartbeat was rejected (OK=false), master doesn't know about us
			// This happens when master restarts and loses worker registrations
			if !ok {
				log.Printf("[WORKER:%s] Heartbeat rejected - master doesn't recognize worker, re-registering", n.id)

				if err := n.reregister(ctx); err != nil {
					log.Printf("[WORKER:%s] Re-registration failed: %v", n.id, err)
				} else {
					log.Printf("[WORKER:%s] Re-registration successful", n.id)
				}
			}
		}
	}
}

// reregister attempts to re-register with the master
func (n *Node) reregister(ctx context.Context) error {
	executorOptions := executors.ListExecutors()
	dataEndpoint := n.server.GetEndpoint()

	regResp, err := n.client.Register(ctx, n.id, protocol.ToyReduceVersion, executorOptions, dataEndpoint)
	if err != nil {
		return fmt.Errorf("registration failed: %w", err)
	}

	log.Printf("[WORKER:%s] Re-registered with master (store: %s)", n.id, regResp.StoreURL)

	return nil
}
