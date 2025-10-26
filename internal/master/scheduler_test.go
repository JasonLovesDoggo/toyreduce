package master

import (
	"testing"
	"time"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
)

// createTestMaster creates a minimal master for testing
func createTestMaster() *Master {
	return &Master{
		storeURL:         "http://localhost:8081",
		heartbeatTimeout: 30 * time.Second,
		storage:          NewNoOpStorage(),
		jobs:             make(map[string]*protocol.Job),
		jobStates:        make(map[string]*JobState),
		jobQueue:         []string{},
		workers:          make(map[string]*WorkerInfo),
	}
}

func TestGetNextTask_NoJob(t *testing.T) {
	m := createTestMaster()

	task := m.GetNextTask("worker-1")

	if task.Type != protocol.TaskTypeNone {
		t.Errorf("Expected TaskTypeNone when no job, got %v", task.Type)
	}
}

func TestGetNextTask_MapTask(t *testing.T) {
	m := createTestMaster()

	// Setup a job with map tasks
	jobID := "job-1"
	m.currentJobID = jobID
	m.jobs[jobID] = &protocol.Job{
		ID:     jobID,
		Status: protocol.JobStatusRunning,
	}
	m.jobStates[jobID] = &JobState{
		mapTasks: []*protocol.MapTask{
			{
				ID:       "map-1",
				Executor: "wordcount",
				Status:   protocol.TaskStatusIdle,
				Chunk:    []string{"line1", "line2"},
			},
		},
		mapTasksLeft:       1,
		mapWorkerEndpoints: make(map[string]string),
	}
	m.workers["worker-1"] = &WorkerInfo{
		ID:           "worker-1",
		DataEndpoint: "http://localhost:9000",
	}

	// Get next task
	task := m.GetNextTask("worker-1")

	// Should be a map task
	if task.Type != protocol.TaskTypeMap {
		t.Fatalf("Expected TaskTypeMap, got %v", task.Type)
	}

	if task.MapTask == nil {
		t.Fatal("MapTask is nil")
	}

	if task.MapTask.ID != "map-1" {
		t.Errorf("Got map task %s, want map-1", task.MapTask.ID)
	}

	// Task should now be in progress
	if task.MapTask.Status != protocol.TaskStatusInProgress {
		t.Errorf("Task status = %v, want InProgress", task.MapTask.Status)
	}

	// Worker should be assigned
	if task.MapTask.WorkerID != "worker-1" {
		t.Errorf("Task worker = %v, want worker-1", task.MapTask.WorkerID)
	}

	// Version should be set for idempotency
	if task.MapTask.Version == "" {
		t.Error("Task version not set")
	}
}

func TestGetNextTask_ReduceTask(t *testing.T) {
	m := createTestMaster()

	// Setup a job in reduce phase (all map tasks done)
	jobID := "job-1"
	m.currentJobID = jobID
	m.jobs[jobID] = &protocol.Job{
		ID:     jobID,
		Status: protocol.JobStatusRunning,
	}
	m.jobStates[jobID] = &JobState{
		mapTasks:     []*protocol.MapTask{}, // No map tasks
		mapTasksLeft: 0,                     // All done
		reduceTasks: []*protocol.ReduceTask{
			{
				ID:        "reduce-1",
				Executor:  "wordcount",
				Status:    protocol.TaskStatusIdle,
				Partition: 0,
			},
		},
		reduceTasksLeft:    1,
		mapWorkerEndpoints: make(map[string]string),
	}
	m.workers["worker-1"] = &WorkerInfo{
		ID: "worker-1",
	}

	// Get next task
	task := m.GetNextTask("worker-1")

	// Should be a reduce task
	if task.Type != protocol.TaskTypeReduce {
		t.Fatalf("Expected TaskTypeReduce, got %v", task.Type)
	}

	if task.ReduceTask == nil {
		t.Fatal("ReduceTask is nil")
	}

	if task.ReduceTask.ID != "reduce-1" {
		t.Errorf("Got reduce task %s, want reduce-1", task.ReduceTask.ID)
	}

	// Task should be in progress
	if task.ReduceTask.Status != protocol.TaskStatusInProgress {
		t.Errorf("Task status = %v, want InProgress", task.ReduceTask.Status)
	}
}

func TestGetNextTask_NoIdleTasks(t *testing.T) {
	m := createTestMaster()

	// Setup a job where all tasks are already assigned
	jobID := "job-1"
	m.currentJobID = jobID
	m.jobs[jobID] = &protocol.Job{
		ID:     jobID,
		Status: protocol.JobStatusRunning,
	}
	m.jobStates[jobID] = &JobState{
		mapTasks: []*protocol.MapTask{
			{
				ID:       "map-1",
				Status:   protocol.TaskStatusInProgress,
				WorkerID: "worker-2",
			},
		},
		mapTasksLeft:       1,
		mapWorkerEndpoints: make(map[string]string),
	}

	// Get next task
	task := m.GetNextTask("worker-1")

	// Should return no task
	if task.Type != protocol.TaskTypeNone {
		t.Errorf("Expected TaskTypeNone, got %v", task.Type)
	}
}

func TestCompleteMapTask_Success(t *testing.T) {
	m := createTestMaster()

	// Setup job with map task
	jobID := "job-1"
	taskID := "map-1"
	version := "v1"

	m.currentJobID = jobID
	m.jobs[jobID] = &protocol.Job{
		ID:            jobID,
		Status:        protocol.JobStatusRunning,
		MapTasksTotal: 1,
	}
	m.jobStates[jobID] = &JobState{
		mapTasks: []*protocol.MapTask{
			{
				ID:       taskID,
				Status:   protocol.TaskStatusInProgress,
				WorkerID: "worker-1",
				Version:  version,
			},
		},
		mapTasksLeft:       1,
		mapWorkerEndpoints: make(map[string]string),
	}
	m.workers["worker-1"] = &WorkerInfo{
		ID:           "worker-1",
		DataEndpoint: "http://localhost:9000",
		CurrentTask:  taskID,
	}

	// Complete the task
	success := m.CompleteMapTask(taskID, "worker-1", version, true, "")

	if !success {
		t.Fatal("CompleteMapTask returned false, want true")
	}

	// Verify task status
	task := m.jobStates[jobID].mapTasks[0]
	if task.Status != protocol.TaskStatusCompleted {
		t.Errorf("Task status = %v, want Completed", task.Status)
	}

	// Verify counters
	if m.jobStates[jobID].mapTasksLeft != 0 {
		t.Errorf("mapTasksLeft = %d, want 0", m.jobStates[jobID].mapTasksLeft)
	}

	if m.jobs[jobID].MapTasksDone != 1 {
		t.Errorf("MapTasksDone = %d, want 1", m.jobs[jobID].MapTasksDone)
	}

	// Verify worker endpoint recorded
	endpoint := m.jobStates[jobID].mapWorkerEndpoints[taskID]
	if endpoint != "http://localhost:9000" {
		t.Errorf("Worker endpoint = %v, want http://localhost:9000", endpoint)
	}

	// Verify worker is freed
	if m.workers["worker-1"].CurrentTask != "" {
		t.Errorf("Worker still has task assigned: %s", m.workers["worker-1"].CurrentTask)
	}
}

func TestCompleteMapTask_Failure(t *testing.T) {
	m := createTestMaster()

	// Setup job with map task
	jobID := "job-1"
	taskID := "map-1"
	version := "v1"

	m.currentJobID = jobID
	m.jobs[jobID] = &protocol.Job{
		ID:     jobID,
		Status: protocol.JobStatusRunning,
	}
	m.jobStates[jobID] = &JobState{
		mapTasks: []*protocol.MapTask{
			{
				ID:         taskID,
				Status:     protocol.TaskStatusInProgress,
				WorkerID:   "worker-1",
				Version:    version,
				RetryCount: 0,
			},
		},
		mapTasksLeft:       1,
		mapWorkerEndpoints: make(map[string]string),
	}
	m.workers["worker-1"] = &WorkerInfo{
		ID:          "worker-1",
		CurrentTask: taskID,
	}

	// Fail the task
	success := m.CompleteMapTask(taskID, "worker-1", version, false, "test error")

	if !success {
		t.Fatal("CompleteMapTask returned false, want true (acknowledged)")
	}

	// Task should be reset to idle for retry
	task := m.jobStates[jobID].mapTasks[0]
	if task.Status != protocol.TaskStatusIdle {
		t.Errorf("Task status = %v, want Idle for retry", task.Status)
	}

	if task.RetryCount != 1 {
		t.Errorf("RetryCount = %d, want 1", task.RetryCount)
	}

	// Worker should be cleared
	if task.WorkerID != "" {
		t.Errorf("WorkerID should be cleared, got %s", task.WorkerID)
	}
}

func TestCompleteMapTask_VersionMismatch(t *testing.T) {
	m := createTestMaster()

	// Setup job with map task
	jobID := "job-1"
	taskID := "map-1"

	m.currentJobID = jobID
	m.jobs[jobID] = &protocol.Job{
		ID:     jobID,
		Status: protocol.JobStatusRunning,
	}
	m.jobStates[jobID] = &JobState{
		mapTasks: []*protocol.MapTask{
			{
				ID:       taskID,
				Status:   protocol.TaskStatusInProgress,
				WorkerID: "worker-1",
				Version:  "v1",
			},
		},
		mapTasksLeft:       1,
		mapWorkerEndpoints: make(map[string]string),
	}

	// Try to complete with wrong version
	success := m.CompleteMapTask(taskID, "worker-1", "v2", true, "")

	if success {
		t.Error("CompleteMapTask should return false for version mismatch")
	}

	// Task should still be in progress
	task := m.jobStates[jobID].mapTasks[0]
	if task.Status != protocol.TaskStatusInProgress {
		t.Errorf("Task status = %v, should remain InProgress", task.Status)
	}
}

func TestCompleteMapTask_MaxRetries(t *testing.T) {
	m := createTestMaster()

	// Setup job with task that has already retried twice
	jobID := "job-1"
	taskID := "map-1"
	version := "v1"

	m.currentJobID = jobID
	m.jobs[jobID] = &protocol.Job{
		ID:     jobID,
		Status: protocol.JobStatusRunning,
	}
	m.jobStates[jobID] = &JobState{
		mapTasks: []*protocol.MapTask{
			{
				ID:         taskID,
				Status:     protocol.TaskStatusInProgress,
				WorkerID:   "worker-1",
				Version:    version,
				RetryCount: 2, // Already tried twice
			},
		},
		mapTasksLeft:       1,
		mapWorkerEndpoints: make(map[string]string),
	}
	m.workers["worker-1"] = &WorkerInfo{ID: "worker-1"}

	// Fail the task (3rd failure)
	m.CompleteMapTask(taskID, "worker-1", version, false, "test error")

	// Task should be marked as failed permanently (retry limit reached)
	task := m.jobStates[jobID].mapTasks[0]
	if task.Status != protocol.TaskStatusFailed {
		t.Errorf("Task status = %v, want Failed (max retries)", task.Status)
	}

	if task.RetryCount != 3 {
		t.Errorf("RetryCount = %d, want 3", task.RetryCount)
	}
}

func TestCompleteReduceTask_Success(t *testing.T) {
	m := createTestMaster()

	// Setup job with reduce task
	jobID := "job-1"
	taskID := "reduce-1"
	version := "v1"

	m.currentJobID = jobID
	m.jobs[jobID] = &protocol.Job{
		ID:               jobID,
		Status:           protocol.JobStatusRunning,
		ReduceTasksTotal: 1,
	}
	m.jobStates[jobID] = &JobState{
		reduceTasks: []*protocol.ReduceTask{
			{
				ID:        taskID,
				Status:    protocol.TaskStatusInProgress,
				WorkerID:  "worker-1",
				Version:   version,
				Partition: 0,
			},
		},
		reduceTasksLeft:    1,
		mapWorkerEndpoints: make(map[string]string),
	}
	m.workers["worker-1"] = &WorkerInfo{
		ID:          "worker-1",
		CurrentTask: taskID,
	}

	// Complete the task
	success := m.CompleteReduceTask(taskID, "worker-1", version, true, "")

	if !success {
		t.Fatal("CompleteReduceTask returned false, want true")
	}

	// Verify task status
	task := m.jobStates[jobID].reduceTasks[0]
	if task.Status != protocol.TaskStatusCompleted {
		t.Errorf("Task status = %v, want Completed", task.Status)
	}

	// Verify counters
	if m.jobStates[jobID].reduceTasksLeft != 0 {
		t.Errorf("reduceTasksLeft = %d, want 0", m.jobStates[jobID].reduceTasksLeft)
	}

	if m.jobs[jobID].ReduceTasksDone != 1 {
		t.Errorf("ReduceTasksDone = %d, want 1", m.jobs[jobID].ReduceTasksDone)
	}

	// Verify worker is freed
	if m.workers["worker-1"].CurrentTask != "" {
		t.Errorf("Worker still has task assigned: %s", m.workers["worker-1"].CurrentTask)
	}
}
