package master

import (
	"log"
	"time"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
)

// StartHealthMonitor starts a goroutine that monitors worker health
func (m *Master) StartHealthMonitor(heartbeatTimeout time.Duration) {
	ticker := time.NewTicker(5 * time.Second)

	go func() {
		for range ticker.C {
			m.checkWorkerHealth(heartbeatTimeout)
		}
	}()

	log.Printf("[MASTER] Health monitor started (timeout: %v)", heartbeatTimeout)
}

// checkWorkerHealth checks for dead workers and stuck tasks
func (m *Master) checkWorkerHealth(timeout time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	deadWorkers := []string{}

	for workerID, worker := range m.workers {
		// Check if worker hasn't sent heartbeat
		if now.Sub(worker.LastHeartbeat) > timeout {
			log.Printf("[MASTER] Worker %s is dead (last heartbeat: %v ago)",
				workerID, now.Sub(worker.LastHeartbeat))

			deadWorkers = append(deadWorkers, workerID)

			// Requeue the worker's current task
			if worker.CurrentTask != "" {
				m.requeueTask(worker.CurrentTask)
			}
		} else if worker.CurrentTask != "" {
			// Check if task has been running too long (5 minutes)
			if now.Sub(worker.InProgressSince) > 5*time.Minute {
				log.Printf("[MASTER] Task %s stuck on worker %s for %v, requeuing",
					worker.CurrentTask, workerID, now.Sub(worker.InProgressSince))
				m.requeueTask(worker.CurrentTask)
				worker.CurrentTask = ""
			}
		}
	}

	// Remove dead workers
	for _, workerID := range deadWorkers {
		delete(m.workers, workerID)
		log.Printf("[MASTER] Removed dead worker %s", workerID)
	}
}

// requeueTask resets a task back to idle status (called with lock held)
func (m *Master) requeueTask(taskID string) {
	// No current job
	if m.currentJobID == "" {
		return
	}

	state := m.jobStates[m.currentJobID]

	// Try to find in map tasks
	for _, task := range state.mapTasks {
		if task.ID == taskID {
			if task.Status == protocol.TaskStatusInProgress {
				task.Status = protocol.TaskStatusIdle
				task.WorkerID = ""
				task.RetryCount++
				log.Printf("[MASTER] Job %s: Requeued map task %s (retry %d)",
					m.currentJobID, taskID, task.RetryCount)
			}

			return
		}
	}

	// Try to find in reduce tasks
	for _, task := range state.reduceTasks {
		if task.ID == taskID {
			if task.Status == protocol.TaskStatusInProgress {
				task.Status = protocol.TaskStatusIdle
				task.WorkerID = ""
				task.RetryCount++
				log.Printf("[MASTER] Job %s: Requeued reduce task %s (retry %d)",
					m.currentJobID, taskID, task.RetryCount)
			}

			return
		}
	}
}
