package master

import (
	"log"
	"time"

	"github.com/google/uuid"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
)

// GetNextTask returns the next available task for a worker
func (m *Master) GetNextTask(workerID string) protocol.Task {
	m.mu.Lock()
	defer m.mu.Unlock()

	// No current job, no tasks
	if m.currentJobID == "" {
		return protocol.Task{Type: protocol.TaskTypeNone}
	}

	job := m.jobs[m.currentJobID]
	state := m.jobStates[m.currentJobID]

	// Try to assign a map task first
	if job.Status == protocol.JobStatusRunning && state.mapTasksLeft > 0 {
		for _, task := range state.mapTasks {
			if task.Status == protocol.TaskStatusIdle {
				return m.assignMapTask(task, workerID)
			}
		}
	}

	// Try to assign a reduce task
	if job.Status == protocol.JobStatusRunning && state.mapTasksLeft == 0 {
		for _, task := range state.reduceTasks {
			if task.Status == protocol.TaskStatusIdle {
				return m.assignReduceTask(task, workerID)
			}
		}
	}

	// No tasks available
	return protocol.Task{Type: protocol.TaskTypeNone}
}

// assignMapTask assigns a map task to a worker
func (m *Master) assignMapTask(task *protocol.MapTask, workerID string) protocol.Task {
	task.Status = protocol.TaskStatusInProgress
	task.WorkerID = workerID
	task.StartTime = time.Now()
	task.Version = uuid.New().String() // New version for idempotency

	// Update worker info
	if worker, exists := m.workers[workerID]; exists {
		worker.CurrentTask = task.ID
		worker.InProgressSince = time.Now()
	}

	log.Printf("[MASTER] Assigned map task %s to worker %s (chunk lines: %d)",
		task.ID, workerID, len(task.Chunk))

	return protocol.Task{
		Type:    protocol.TaskTypeMap,
		MapTask: task,
	}
}

// assignReduceTask assigns a reduce task to a worker
func (m *Master) assignReduceTask(task *protocol.ReduceTask, workerID string) protocol.Task {
	task.Status = protocol.TaskStatusInProgress
	task.WorkerID = workerID
	task.StartTime = time.Now()
	task.Version = uuid.New().String() // New version for idempotency

	// Update worker info
	if worker, exists := m.workers[workerID]; exists {
		worker.CurrentTask = task.ID
		worker.InProgressSince = time.Now()
	}

	log.Printf("[MASTER] Assigned reduce task %s (partition %d) to worker %s",
		task.ID, task.Partition, workerID)

	return protocol.Task{
		Type:       protocol.TaskTypeReduce,
		ReduceTask: task,
	}
}

// CompleteMapTask marks a map task as completed
func (m *Master) CompleteMapTask(taskID, workerID, version string, success bool, errorMsg string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	// No current job
	if m.currentJobID == "" {
		return false
	}

	state := m.jobStates[m.currentJobID]
	job := m.jobs[m.currentJobID]

	// Find the task
	var task *protocol.MapTask

	for _, t := range state.mapTasks {
		if t.ID == taskID {
			task = t
			break
		}
	}

	if task == nil {
		log.Printf("[MASTER] Map task %s not found", taskID)
		return false
	}

	// Check version for idempotency
	if task.Version != version {
		log.Printf("[MASTER] Map task %s version mismatch (expected %s, got %s)",
			taskID, task.Version, version)

		return false
	}

	// Check if task is still in progress
	if task.Status != protocol.TaskStatusInProgress {
		log.Printf("[MASTER] Map task %s not in progress (status: %s)", taskID, task.Status)
		return false
	}

	// Update task status
	if success {
		task.Status = protocol.TaskStatusCompleted
		task.CompletedAt = time.Now()
		state.mapTasksLeft--
		job.MapTasksDone++
		job.CompletedTasks++

		log.Printf("[MASTER] Job %s: Map task %s completed by worker %s (%d tasks left)",
			m.currentJobID, taskID, workerID, state.mapTasksLeft)

		// Record worker endpoint for this map task
		if worker, exists := m.workers[workerID]; exists {
			if state.mapWorkerEndpoints == nil {
				state.mapWorkerEndpoints = make(map[string]string)
			}

			state.mapWorkerEndpoints[taskID] = worker.DataEndpoint
		}

		// Persist task completion
		if err := m.storage.SaveJob(job); err != nil {
			log.Printf("[MASTER] Warning: Failed to persist job: %v", err)
		}

		if err := m.storage.SaveJobState(m.currentJobID, state); err != nil {
			log.Printf("[MASTER] Warning: Failed to persist job state: %v", err)
		}

		// Check if we should transition to reduce phase
		if state.mapTasksLeft == 0 {
			go m.transitionToReducePhase()
		}
	} else {
		task.Status = protocol.TaskStatusFailed
		task.RetryCount++
		log.Printf("[MASTER] Job %s: Map task %s failed: %s (retry %d)",
			m.currentJobID, taskID, errorMsg, task.RetryCount)

		// Reset to idle for retry
		if task.RetryCount < 3 {
			task.Status = protocol.TaskStatusIdle
			task.WorkerID = ""
		}
	}

	// Update worker info
	if worker, exists := m.workers[workerID]; exists {
		worker.CurrentTask = ""
	}

	return true
}

// CompleteReduceTask marks a reduce task as completed
func (m *Master) CompleteReduceTask(taskID, workerID, version string, success bool, errorMsg string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	// No current job
	if m.currentJobID == "" {
		return false
	}

	state := m.jobStates[m.currentJobID]
	job := m.jobs[m.currentJobID]

	// Find the task
	var task *protocol.ReduceTask

	for _, t := range state.reduceTasks {
		if t.ID == taskID {
			task = t
			break
		}
	}

	if task == nil {
		log.Printf("[MASTER] Reduce task %s not found", taskID)
		return false
	}

	// Check version for idempotency
	if task.Version != version {
		log.Printf("[MASTER] Reduce task %s version mismatch (expected %s, got %s)",
			taskID, task.Version, version)

		return false
	}

	// Check if task is still in progress
	if task.Status != protocol.TaskStatusInProgress {
		log.Printf("[MASTER] Reduce task %s not in progress (status: %s)", taskID, task.Status)
		return false
	}

	// Update task status
	if success {
		task.Status = protocol.TaskStatusCompleted
		task.CompletedAt = time.Now()
		state.reduceTasksLeft--
		job.ReduceTasksDone++
		job.CompletedTasks++

		log.Printf("[MASTER] Job %s: Reduce task %s (partition %d) completed by worker %s (%d tasks left)",
			m.currentJobID, taskID, task.Partition, workerID, state.reduceTasksLeft)

		// Persist task completion
		if err := m.storage.SaveJob(job); err != nil {
			log.Printf("[MASTER] Warning: Failed to persist job: %v", err)
		}

		if err := m.storage.SaveJobState(m.currentJobID, state); err != nil {
			log.Printf("[MASTER] Warning: Failed to persist job state: %v", err)
		}

		// Check if job is complete
		if state.reduceTasksLeft == 0 {
			go m.checkJobCompletion()
		}
	} else {
		task.Status = protocol.TaskStatusFailed
		task.RetryCount++
		log.Printf("[MASTER] Job %s: Reduce task %s failed: %s (retry %d)",
			m.currentJobID, taskID, errorMsg, task.RetryCount)

		// Reset to idle for retry
		if task.RetryCount < 3 {
			task.Status = protocol.TaskStatusIdle
			task.WorkerID = ""
		}
	}

	// Update worker info
	if worker, exists := m.workers[workerID]; exists {
		worker.CurrentTask = ""
	}

	return true
}
