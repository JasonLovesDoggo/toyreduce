package master

import (
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
)

// WorkerInfo tracks information about a registered worker
type WorkerInfo struct {
	ID              string
	LastHeartbeat   time.Time
	CurrentTask     string
	InProgressSince time.Time
}

// Master coordinates the MapReduce job
type Master struct {
	// Configuration
	cacheURL       string
	inputPath      string
	chunkSize      int
	numReduceTasks int
	worker         toyreduce.Worker
	executorName   string

	// Task tracking
	mapTasks        []*protocol.MapTask
	reduceTasks     []*protocol.ReduceTask
	mapTasksLeft    int
	reduceTasksLeft int

	// Worker registry
	workers map[string]*WorkerInfo

	// Job state
	jobStatus string // "chunking", "mapping", "reducing", "completed", "failed"
	startTime time.Time
	endTime   time.Time

	mu sync.RWMutex
}

// Config holds master configuration
type Config struct {
	Port             int
	CacheURL         string
	InputPath        string
	ChunkSize        int
	ReduceTasks      int
	Worker           toyreduce.Worker
	ExecutorName     string
	HeartbeatTimeout time.Duration
}

// NewMaster creates a new master instance
func NewMaster(cfg Config) (*Master, error) {
	m := &Master{
		cacheURL:       cfg.CacheURL,
		inputPath:      cfg.InputPath,
		chunkSize:      cfg.ChunkSize,
		numReduceTasks: cfg.ReduceTasks,
		worker:         cfg.Worker,
		executorName:   cfg.ExecutorName,
		workers:        make(map[string]*WorkerInfo),
		jobStatus:      "initializing",
		startTime:      time.Now(),
	}

	// Initialize map tasks by chunking input file
	if err := m.initializeMapTasks(); err != nil {
		return nil, err
	}

	m.jobStatus = "mapping"
	log.Printf("[MASTER] Initialized with %d map tasks, %d reduce tasks", len(m.mapTasks), cfg.ReduceTasks)

	return m, nil
}

// initializeMapTasks chunks the input file and creates map tasks
func (m *Master) initializeMapTasks() error {
	chunks := make(chan []string, 100)

	go func() {
		if err := toyreduce.Chunk(m.inputPath, m.chunkSize, chunks); err != nil {
			log.Printf("[MASTER] Error chunking file: %v", err)
		}
	}()

	var tasks []*protocol.MapTask
	for chunk := range chunks {
		task := &protocol.MapTask{
			ID:            uuid.New().String(),
			Chunk:         chunk,
			Status:        protocol.TaskStatusIdle,
			Version:       uuid.New().String(),
			NumPartitions: m.numReduceTasks,
		}
		tasks = append(tasks, task)
	}

	m.mapTasks = tasks
	m.mapTasksLeft = len(tasks)

	return nil
}

// RegisterWorker registers a new worker
func (m *Master) RegisterWorker(workerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.workers[workerID] = &WorkerInfo{
		ID:            workerID,
		LastHeartbeat: time.Now(),
	}
	log.Printf("[MASTER] Worker registered: %s (total: %d)", workerID, len(m.workers))
}

// UpdateHeartbeat updates worker's last heartbeat time
func (m *Master) UpdateHeartbeat(workerID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	worker, exists := m.workers[workerID]
	if !exists {
		return false
	}

	worker.LastHeartbeat = time.Now()
	return true
}

// GetStatus returns the current job status
func (m *Master) GetStatus() protocol.StatusResponse {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := protocol.StatusResponse{
		MapTasksTotal:     len(m.mapTasks),
		ReduceTasksTotal:  len(m.reduceTasks),
		WorkersRegistered: len(m.workers),
		JobStatus:         m.jobStatus,
	}

	// Count map task statuses
	for _, task := range m.mapTasks {
		switch task.Status {
		case protocol.TaskStatusIdle:
			status.MapTasksIdle++
		case protocol.TaskStatusInProgress:
			status.MapTasksInProgress++
		case protocol.TaskStatusCompleted:
			status.MapTasksCompleted++
		case protocol.TaskStatusFailed:
			status.MapTasksFailed++
		}
	}

	// Count reduce task statuses
	for _, task := range m.reduceTasks {
		switch task.Status {
		case protocol.TaskStatusIdle:
			status.ReduceTasksIdle++
		case protocol.TaskStatusInProgress:
			status.ReduceTasksInProgress++
		case protocol.TaskStatusCompleted:
			status.ReduceTasksCompleted++
		case protocol.TaskStatusFailed:
			status.ReduceTasksFailed++
		}
	}

	// Count active workers (heartbeat within last 30s)
	now := time.Now()
	for _, worker := range m.workers {
		if now.Sub(worker.LastHeartbeat) < 30*time.Second {
			status.WorkersActive++
		}
	}

	return status
}

// transitionToReducePhase creates reduce tasks after all map tasks complete
func (m *Master) transitionToReducePhase() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.jobStatus != "mapping" {
		return
	}

	log.Printf("[MASTER] All map tasks completed, transitioning to reduce phase")
	m.jobStatus = "reducing"

	// Create reduce tasks (one per partition)
	for i := 0; i < m.numReduceTasks; i++ {
		task := &protocol.ReduceTask{
			ID:        uuid.New().String(),
			Partition: i,
			Status:    protocol.TaskStatusIdle,
			Version:   uuid.New().String(),
		}
		m.reduceTasks = append(m.reduceTasks, task)
	}

	m.reduceTasksLeft = len(m.reduceTasks)
	log.Printf("[MASTER] Created %d reduce tasks", len(m.reduceTasks))
}

// checkJobCompletion checks if all tasks are done
func (m *Master) checkJobCompletion() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.jobStatus == "reducing" && m.reduceTasksLeft == 0 {
		m.jobStatus = "completed"
		m.endTime = time.Now()
		duration := m.endTime.Sub(m.startTime)
		log.Printf("[MASTER] Job completed in %v", duration)
	}
}
