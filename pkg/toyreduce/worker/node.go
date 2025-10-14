package worker

import (
	"log"
	"time"

	"github.com/google/uuid"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
	"pkg.jsn.cam/toyreduce/workers"
)

// Config holds worker configuration
type Config struct {
	MasterURL         string
	PollInterval      time.Duration
	HeartbeatInterval time.Duration
}

// Node represents a worker node
type Node struct {
	id        string
	client    *Client
	processor *Processor
	config    Config
}

// NewNode creates a new worker node
func NewNode(cfg Config) *Node {
	workerID := uuid.New().String()
	client := NewClient(cfg.MasterURL)

	return &Node{
		id:     workerID,
		client: client,
		config: cfg,
	}
}

// Start starts the worker main loop
func (n *Node) Start() error {
	log.Printf("[WORKER:%s] Starting worker", n.id)

	// Register with master
	regResp, err := n.client.Register(n.id)
	if err != nil {
		return err
	}

	log.Printf("[WORKER:%s] Registered with master (cache: %s, executor: %s)",
		n.id, regResp.CacheURL, regResp.ExecutorName)

	// Get the worker implementation
	worker := getWorkerByName(regResp.ExecutorName)
	if worker == nil {
		log.Fatalf("[WORKER:%s] Unknown executor: %s", n.id, regResp.ExecutorName)
	}

	n.processor = NewProcessor(worker, n.client)

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
