package master

import (
	"log"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
	"pkg.jsn.cam/toyreduce/pkg/workers"
)

// persist saves the current state to storage
func (m *Master) persist() error {
	if m.storage == nil {
		return nil // No persistence
	}

	// Persist jobs
	for _, job := range m.jobs {
		if err := m.storage.SaveJob(job); err != nil {
			log.Printf("[MASTER] Error persisting job %s: %v", job.ID, err)
		}
	}

	// Persist job states
	for jobID, state := range m.jobStates {
		if err := m.storage.SaveJobState(jobID, state); err != nil {
			log.Printf("[MASTER] Error persisting job state %s: %v", jobID, err)
		}
	}

	// Persist queue
	if err := m.storage.SaveQueue(m.jobQueue); err != nil {
		log.Printf("[MASTER] Error persisting queue: %v", err)
	}

	// Persist current job ID
	if err := m.storage.SaveCurrentJobID(m.currentJobID); err != nil {
		log.Printf("[MASTER] Error persisting current job ID: %v", err)
	}

	return nil
}

// restore loads state from storage
func (m *Master) restore() error {
	if m.storage == nil {
		return nil // No persistence
	}

	log.Printf("[MASTER] Restoring state from storage...")

	// Load jobs
	jobs, err := m.storage.LoadJobs()
	if err != nil {
		return err
	}
	m.jobs = jobs
	log.Printf("[MASTER] Restored %d jobs", len(jobs))

	// Load job states
	jobStates, err := m.storage.LoadJobStates()
	if err != nil {
		return err
	}

	// Restore worker implementations for each job state
	for jobID, state := range jobStates {
		job, exists := m.jobs[jobID]
		if !exists {
			log.Printf("[MASTER] Warning: Job state exists for non-existent job %s", jobID)
			continue
		}

		// Restore worker implementation
		worker := workers.GetWorker(job.Executor)
		if worker == nil {
			log.Printf("[MASTER] Warning: Executor not found for job %s: %s", jobID, job.Executor)
			continue
		}
		state.worker = worker

		m.jobStates[jobID] = state
	}
	log.Printf("[MASTER] Restored %d job states", len(m.jobStates))

	// Load queue
	queue, err := m.storage.LoadQueue()
	if err != nil {
		return err
	}
	m.jobQueue = queue
	log.Printf("[MASTER] Restored queue with %d jobs", len(queue))

	// Load current job ID
	currentJobID, err := m.storage.LoadCurrentJobID()
	if err != nil {
		return err
	}
	m.currentJobID = currentJobID

	// Handle resume logic for running job
	if m.currentJobID != "" {
		if err := m.resumeCurrentJob(); err != nil {
			log.Printf("[MASTER] Error resuming job %s: %v", m.currentJobID, err)
		}
	}

	// Start next job if queue has items and no job is running
	m.startNextJobIfReady()

	log.Printf("[MASTER] State restore complete")
	return nil
}

// resumeCurrentJob handles resuming a job after master restart
func (m *Master) resumeCurrentJob() error {
	job, exists := m.jobs[m.currentJobID]
	if !exists {
		return nil // Job doesn't exist, clear current
	}

	state, exists := m.jobStates[m.currentJobID]
	if !exists {
		// No state, mark job as failed
		log.Printf("[MASTER] Job %s has no state, marking as failed", m.currentJobID)
		job.Status = protocol.JobStatusFailed
		job.Error = "Master restarted with no job state"
		m.currentJobID = ""
		return nil
	}

	log.Printf("[MASTER] Resuming job %s (executor: %s)", m.currentJobID, job.Executor)

	// Reset all in-progress tasks to idle (workers will retry)
	for _, task := range state.mapTasks {
		if task.Status == protocol.TaskStatusInProgress {
			task.Status = protocol.TaskStatusIdle
			task.WorkerID = ""
			log.Printf("[MASTER] Reset in-progress map task %s to idle", task.ID)
		}
	}

	for _, task := range state.reduceTasks {
		if task.Status == protocol.TaskStatusInProgress {
			task.Status = protocol.TaskStatusIdle
			task.WorkerID = ""
			log.Printf("[MASTER] Reset in-progress reduce task %s to idle", task.ID)
		}
	}

	// Recalculate task counts
	mapTasksLeft := 0
	reduceTasksLeft := 0

	for _, task := range state.mapTasks {
		if task.Status != protocol.TaskStatusCompleted {
			mapTasksLeft++
		}
	}

	for _, task := range state.reduceTasks {
		if task.Status != protocol.TaskStatusCompleted {
			reduceTasksLeft++
		}
	}

	state.mapTasksLeft = mapTasksLeft
	state.reduceTasksLeft = reduceTasksLeft

	log.Printf("[MASTER] Job %s: %d map tasks left, %d reduce tasks left",
		m.currentJobID, mapTasksLeft, reduceTasksLeft)

	// Check if job is actually complete
	if m.isJobComplete(job, state) {
		log.Printf("[MASTER] Job %s is actually complete, marking as such", m.currentJobID)
		job.Status = protocol.JobStatusCompleted
		m.currentJobID = ""
	}

	return nil
}

// Close closes the storage connection
func (m *Master) Close() error {
	if m.storage != nil {
		return m.storage.Close()
	}
	return nil
}
