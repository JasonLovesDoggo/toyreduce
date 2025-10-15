package master

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
	"pkg.jsn.cam/toyreduce/pkg/workers"
)

// WorkerInfo tracks information about a registered worker
type WorkerInfo struct {
	ID              string
	Version         string
	Executors       []string
	LastHeartbeat   time.Time
	CurrentTask     string
	InProgressSince time.Time
}

// JobState holds execution state for a single job
type JobState struct {
	mapTasks        []*protocol.MapTask
	reduceTasks     []*protocol.ReduceTask
	mapTasksLeft    int
	reduceTasksLeft int
	worker          toyreduce.Worker // Executor implementation for this job
}

// Master coordinates MapReduce jobs in a long-running cluster
type Master struct {
	// Configuration
	storeURL         string
	heartbeatTimeout time.Duration
	storage          Storage

	// Job management
	jobs         map[string]*protocol.Job // Job metadata
	jobStates    map[string]*JobState     // Job execution state
	jobQueue     []string                 // Queue of job IDs (queued jobs)
	currentJobID string                   // Currently executing job

	// Worker registry
	workers map[string]*WorkerInfo

	mu sync.RWMutex
}

// Config holds master configuration
type Config struct {
	Port             int
	StoreURL         string
	HeartbeatTimeout time.Duration
	DBPath           string // Path to bbolt database (empty = no persistence)
}

// NewMaster creates a new master instance (no job required)
func NewMaster(cfg Config) (*Master, error) {
	heartbeatTimeout := cfg.HeartbeatTimeout
	if heartbeatTimeout == 0 {
		heartbeatTimeout = 30 * time.Second
	}

	// Initialize storage
	var storage Storage
	if cfg.DBPath != "" {
		bboltStorage, err := NewBboltStorage(cfg.DBPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create storage: %w", err)
		}
		storage = bboltStorage
		log.Printf("[MASTER] Persistence enabled at %s", cfg.DBPath)
	} else {
		storage = NewNoOpStorage()
		log.Printf("[MASTER] Persistence disabled (no DBPath configured)")
	}

	m := &Master{
		storeURL:         cfg.StoreURL,
		heartbeatTimeout: heartbeatTimeout,
		storage:          storage,
		jobs:             make(map[string]*protocol.Job),
		jobStates:        make(map[string]*JobState),
		jobQueue:         []string{},
		workers:          make(map[string]*WorkerInfo),
	}

	// Restore state from storage
	if err := m.restore(); err != nil {
		log.Printf("[MASTER] Warning: Failed to restore state: %v", err)
	}

	log.Printf("[MASTER] Initialized (ready for job submissions)")
	return m, nil
}

// SubmitJob accepts a new job submission
func (m *Master) SubmitJob(req protocol.JobSubmitRequest) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Validate executor exists
	if !workers.IsValidExecutor(req.Executor) {
		return "", fmt.Errorf("unknown executor: %s", req.Executor)
	}

	// Create job
	jobID := uuid.New().String()
	job := &protocol.Job{
		ID:          jobID,
		Status:      protocol.JobStatusQueued,
		Executor:    req.Executor,
		InputPath:   req.InputPath,
		OutputPath:  req.OutputPath,
		ChunkSize:   req.ChunkSize,
		ReduceTasks: req.ReduceTasks,
		SubmittedAt: time.Now(),
	}

	m.jobs[jobID] = job
	m.jobQueue = append(m.jobQueue, jobID)

	log.Printf("[MASTER] Job submitted: %s (executor: %s, queued at position %d)",
		jobID, req.Executor, len(m.jobQueue))

	// Persist changes
	if err := m.storage.SaveJob(job); err != nil {
		log.Printf("[MASTER] Warning: Failed to persist job: %v", err)
	}
	if err := m.storage.SaveQueue(m.jobQueue); err != nil {
		log.Printf("[MASTER] Warning: Failed to persist queue: %v", err)
	}

	// Start job if none running
	m.startNextJobIfReady()

	return jobID, nil
}

// startNextJobIfReady starts the next queued job if no job is running
func (m *Master) startNextJobIfReady() {
	// Must be called with lock held

	if m.currentJobID != "" {
		return // Job already running
	}

	if len(m.jobQueue) == 0 {
		return // No jobs queued
	}

	// Dequeue next job
	jobID := m.jobQueue[0]
	m.jobQueue = m.jobQueue[1:]
	m.currentJobID = jobID

	job := m.jobs[jobID]
	job.Status = protocol.JobStatusRunning
	job.StartedAt = time.Now()

	log.Printf("[MASTER] Starting job: %s (executor: %s)", jobID, job.Executor)

	// Initialize job state
	if err := m.initializeJob(jobID, job); err != nil {
		log.Printf("[MASTER] Failed to initialize job %s: %v", jobID, err)
		job.Status = protocol.JobStatusFailed
		job.Error = err.Error()
		job.CompletedAt = time.Now()
		m.currentJobID = ""

		// Persist failure
		if err := m.storage.SaveJob(job); err != nil {
			log.Printf("[MASTER] Warning: Failed to persist failed job: %v", err)
		}
		if err := m.storage.SaveCurrentJobID(m.currentJobID); err != nil {
			log.Printf("[MASTER] Warning: Failed to persist current job ID: %v", err)
		}

		m.startNextJobIfReady() // Try next job
		return
	}

	log.Printf("[MASTER] Job %s initialized with %d map tasks, %d reduce tasks",
		jobID, len(m.jobStates[jobID].mapTasks), job.ReduceTasks)

	// Persist job start
	if err := m.storage.SaveJob(job); err != nil {
		log.Printf("[MASTER] Warning: Failed to persist job: %v", err)
	}
	if err := m.storage.SaveJobState(jobID, m.jobStates[jobID]); err != nil {
		log.Printf("[MASTER] Warning: Failed to persist job state: %v", err)
	}
	if err := m.storage.SaveCurrentJobID(m.currentJobID); err != nil {
		log.Printf("[MASTER] Warning: Failed to persist current job ID: %v", err)
	}
}

// initializeJob creates the execution state for a job (map tasks, etc)
func (m *Master) initializeJob(jobID string, job *protocol.Job) error {
	// Must be called with lock held

	// Get worker implementation
	worker := workers.GetWorker(job.Executor)
	if worker == nil {
		return fmt.Errorf("executor not found: %s", job.Executor)
	}

	// Chunk input file
	chunks := make(chan []string, 100)
	go func() {
		if err := toyreduce.Chunk(job.InputPath, job.ChunkSize, chunks); err != nil {
			log.Printf("[MASTER] Error chunking file for job %s: %v", jobID, err)
		}
	}()

	var mapTasks []*protocol.MapTask
	for chunk := range chunks {
		task := &protocol.MapTask{
			ID:            uuid.New().String(),
			Chunk:         chunk,
			Status:        protocol.TaskStatusIdle,
			Version:       uuid.New().String(),
			NumPartitions: job.ReduceTasks,
		}
		mapTasks = append(mapTasks, task)
	}

	// Create job state
	state := &JobState{
		mapTasks:     mapTasks,
		reduceTasks:  []*protocol.ReduceTask{},
		mapTasksLeft: len(mapTasks),
		worker:       worker,
	}
	m.jobStates[jobID] = state

	// Update job metadata
	job.MapTasksTotal = len(mapTasks)
	job.ReduceTasksTotal = job.ReduceTasks
	job.TotalTasks = job.MapTasksTotal + job.ReduceTasksTotal

	return nil
}

// RegisterWorker registers a new worker with version and capability validation
func (m *Master) RegisterWorker(workerID, version string, executors []string) error {
	// Validate version compatibility
	compatible, err := protocol.IsCompatibleVersion(version, protocol.ToyReduceVersion)
	if err != nil {
		return fmt.Errorf("version validation error: %w", err)
	}
	if !compatible {
		return fmt.Errorf("incompatible version: %s", protocol.GetCompatibilityError(version, protocol.ToyReduceVersion))
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.workers[workerID] = &WorkerInfo{
		ID:            workerID,
		Version:       version,
		Executors:     executors,
		LastHeartbeat: time.Now(),
	}
	log.Printf("[MASTER] Worker registered: %s (version: %s, executors: %v, total: %d)",
		workerID, version, executors, len(m.workers))

	return nil
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

// GetJob returns job metadata
func (m *Master) GetJob(jobID string) *protocol.Job {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.jobs[jobID]
}

// ListJobs returns all jobs
func (m *Master) ListJobs() []protocol.Job {
	m.mu.RLock()
	defer m.mu.RUnlock()

	jobs := make([]protocol.Job, 0, len(m.jobs))
	for _, job := range m.jobs {
		jobs = append(jobs, *job)
	}
	return jobs
}

// CancelJob cancels a queued or running job
func (m *Master) CancelJob(jobID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	job, exists := m.jobs[jobID]
	if !exists {
		return fmt.Errorf("job not found: %s", jobID)
	}

	if job.Status == protocol.JobStatusCompleted || job.Status == protocol.JobStatusFailed {
		return fmt.Errorf("job already finished: %s", job.Status)
	}

	if job.Status == protocol.JobStatusCancelled {
		return fmt.Errorf("job already cancelled")
	}

	// Mark as cancelled
	job.Status = protocol.JobStatusCancelled
	job.CompletedAt = time.Now()

	// If it's the current job, move to next
	if m.currentJobID == jobID {
		m.currentJobID = ""
		m.startNextJobIfReady()
	} else {
		// Remove from queue if queued
		for i, queuedID := range m.jobQueue {
			if queuedID == jobID {
				m.jobQueue = append(m.jobQueue[:i], m.jobQueue[i+1:]...)
				break
			}
		}
	}

	// Persist cancellation
	if err := m.storage.SaveJob(job); err != nil {
		log.Printf("[MASTER] Warning: Failed to persist cancelled job: %v", err)
	}
	if err := m.storage.SaveQueue(m.jobQueue); err != nil {
		log.Printf("[MASTER] Warning: Failed to persist queue: %v", err)
	}
	if err := m.storage.SaveCurrentJobID(m.currentJobID); err != nil {
		log.Printf("[MASTER] Warning: Failed to persist current job ID: %v", err)
	}

	log.Printf("[MASTER] Job cancelled: %s", jobID)
	return nil
}

// GetJobResults returns the results for a completed job
func (m *Master) GetJobResults(jobID string) ([]toyreduce.KeyValue, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	job, exists := m.jobs[jobID]
	if !exists {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}

	if job.Status != protocol.JobStatusCompleted {
		return nil, fmt.Errorf("job not completed (status: %s)", job.Status)
	}

	return job.Results, nil
}

// GetStatus returns the current job status (for backwards compat)
func (m *Master) GetStatus() protocol.StatusResponse {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := protocol.StatusResponse{
		WorkersRegistered: len(m.workers),
		JobStatus:         "idle",
	}

	// Count active workers (heartbeat within last 30s)
	now := time.Now()
	for _, worker := range m.workers {
		if now.Sub(worker.LastHeartbeat) < 30*time.Second {
			status.WorkersActive++
		}
	}

	// If there's a current job, populate task stats
	if m.currentJobID != "" {
		state := m.jobStates[m.currentJobID]
		job := m.jobs[m.currentJobID]

		status.MapTasksTotal = len(state.mapTasks)
		status.ReduceTasksTotal = len(state.reduceTasks)
		status.JobStatus = string(job.Status)

		// Count map task statuses
		for _, task := range state.mapTasks {
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
		for _, task := range state.reduceTasks {
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
	}

	return status
}

// ListWorkers returns all workers with their current status
func (m *Master) ListWorkers() []protocol.UIWorker {
	m.mu.RLock()
	defer m.mu.RUnlock()

	workers := make([]protocol.UIWorker, 0, len(m.workers))
	now := time.Now()

	for _, worker := range m.workers {
		// Consider worker online if heartbeat within heartbeat timeout
		online := now.Sub(worker.LastHeartbeat) < m.heartbeatTimeout

		workers = append(workers, protocol.UIWorker{
			ID:            worker.ID,
			Executors:     worker.Executors,
			CurrentTask:   worker.CurrentTask,
			LastHeartbeat: worker.LastHeartbeat,
			Online:        online,
		})
	}

	return workers
}

// GetConfig returns the master configuration for the UI
func (m *Master) GetConfig() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return map[string]interface{}{
		"store_url":         m.storeURL,
		"heartbeat_timeout": m.heartbeatTimeout.String(),
	}
}

// transitionToReducePhase creates reduce tasks after all map tasks complete
func (m *Master) transitionToReducePhase() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.currentJobID == "" {
		return
	}

	job := m.jobs[m.currentJobID]
	state := m.jobStates[m.currentJobID]

	if job.Status != protocol.JobStatusRunning || state.mapTasksLeft > 0 {
		return
	}

	if len(state.reduceTasks) > 0 {
		return // Already transitioned
	}

	log.Printf("[MASTER] Job %s: All map tasks completed, transitioning to reduce phase", m.currentJobID)

	// Mark map phase as complete
	job.MapPhaseCompletedAt = time.Now()

	// Create reduce tasks (one per partition)
	for i := 0; i < job.ReduceTasks; i++ {
		task := &protocol.ReduceTask{
			ID:        uuid.New().String(),
			JobID:     m.currentJobID,
			Partition: i,
			Status:    protocol.TaskStatusIdle,
			Version:   uuid.New().String(),
		}
		state.reduceTasks = append(state.reduceTasks, task)
	}

	state.reduceTasksLeft = len(state.reduceTasks)
	log.Printf("[MASTER] Job %s: Created %d reduce tasks", m.currentJobID, len(state.reduceTasks))

	// Persist reduce phase transition
	if err := m.storage.SaveJob(job); err != nil {
		log.Printf("[MASTER] Warning: Failed to persist job: %v", err)
	}
	if err := m.storage.SaveJobState(m.currentJobID, state); err != nil {
		log.Printf("[MASTER] Warning: Failed to persist job state: %v", err)
	}
}

// isJobComplete checks if a job is complete based on its current state
func (m *Master) isJobComplete(job *protocol.Job, state *JobState) bool {
	if job.Status != protocol.JobStatusRunning {
		return false
	}

	if len(state.reduceTasks) == 0 {
		// Map-only job: complete when all map tasks are done
		return state.mapTasksLeft == 0
	} else {
		// Map-reduce job: complete when all reduce tasks are done
		return state.reduceTasksLeft == 0
	}
}

// completeJob marks a job as completed and handles cleanup
func (m *Master) completeJob(jobID string) {
	job := m.jobs[jobID]
	job.Status = protocol.JobStatusCompleted
	job.CompletedAt = time.Now()
	duration := job.CompletedAt.Sub(job.StartedAt)
	log.Printf("[MASTER] Job %s completed in %v", jobID, duration)

	// Fetch results from store
	go m.fetchJobResults(jobID)

	// Clear current job and start next
	m.currentJobID = ""

	// Persist completion
	if err := m.storage.SaveJob(job); err != nil {
		log.Printf("[MASTER] Warning: Failed to persist completed job: %v", err)
	}
	if err := m.storage.SaveCurrentJobID(m.currentJobID); err != nil {
		log.Printf("[MASTER] Warning: Failed to persist current job ID: %v", err)
	}

	m.startNextJobIfReady()
}

// checkJobCompletion checks if the current job is done and starts the next
func (m *Master) checkJobCompletion() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.currentJobID == "" {
		return
	}

	job := m.jobs[m.currentJobID]
	state := m.jobStates[m.currentJobID]

	if m.isJobComplete(job, state) {
		m.completeJob(m.currentJobID)
	}
}

// fetchJobResults fetches the final results from store and stores them in the job
func (m *Master) fetchJobResults(jobID string) {
	job := m.GetJob(jobID)
	if job == nil {
		return
	}

	// Fetch job-specific results from store
	results, err := m.getJobResultsFromStore(jobID)
	if err != nil {
		log.Printf("[MASTER] Failed to fetch results for job %s: %v", jobID, err)
		return
	}

	// Store results in job
	m.mu.Lock()
	job.Results = results
	m.mu.Unlock()

	log.Printf("[MASTER] Stored %d results for job %s", len(results), jobID)

	// Persist results
	if err := m.storage.SaveJob(job); err != nil {
		log.Printf("[MASTER] Warning: Failed to persist job results: %v", err)
	}
}

// getJobResultsFromStore fetches results for a specific job from the store server
func (m *Master) getJobResultsFromStore(jobID string) ([]toyreduce.KeyValue, error) {
	if m.storeURL == "" {
		return nil, fmt.Errorf("store URL not configured")
	}

	url := fmt.Sprintf("%s/results/job/%s", m.storeURL, jobID)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch results: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("store returned status %d", resp.StatusCode)
	}

	var results []toyreduce.KeyValue
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return nil, fmt.Errorf("failed to decode results: %w", err)
	}

	return results, nil
}
