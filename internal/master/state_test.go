package master

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
)

func TestIsJobComplete(t *testing.T) {
	tests := []struct {
		name            string
		jobStatus       protocol.JobStatus
		mapTasksLeft    int
		reduceTasks     []*protocol.ReduceTask
		reduceTasksLeft int
		expected        bool
	}{
		{
			name:            "map-only job with tasks remaining",
			jobStatus:       protocol.JobStatusRunning,
			mapTasksLeft:    2,
			reduceTasks:     []*protocol.ReduceTask{},
			reduceTasksLeft: 0,
			expected:        false,
		},
		{
			name:            "map-only job complete",
			jobStatus:       protocol.JobStatusRunning,
			mapTasksLeft:    0,
			reduceTasks:     []*protocol.ReduceTask{},
			reduceTasksLeft: 0,
			expected:        true,
		},
		{
			name:            "map-reduce job with map tasks remaining",
			jobStatus:       protocol.JobStatusRunning,
			mapTasksLeft:    1,
			reduceTasks:     []*protocol.ReduceTask{{ID: "reduce-1"}},
			reduceTasksLeft: 1,
			expected:        false,
		},
		{
			name:            "map-reduce job with reduce tasks remaining",
			jobStatus:       protocol.JobStatusRunning,
			mapTasksLeft:    0,
			reduceTasks:     []*protocol.ReduceTask{{ID: "reduce-1"}, {ID: "reduce-2"}},
			reduceTasksLeft: 1,
			expected:        false,
		},
		{
			name:            "map-reduce job complete",
			jobStatus:       protocol.JobStatusRunning,
			mapTasksLeft:    0,
			reduceTasks:     []*protocol.ReduceTask{{ID: "reduce-1"}, {ID: "reduce-2"}},
			reduceTasksLeft: 0,
			expected:        true,
		},
		{
			name:            "job not running",
			jobStatus:       protocol.JobStatusQueued,
			mapTasksLeft:    0,
			reduceTasks:     []*protocol.ReduceTask{},
			reduceTasksLeft: 0,
			expected:        false,
		},
		{
			name:            "job already completed",
			jobStatus:       protocol.JobStatusCompleted,
			mapTasksLeft:    0,
			reduceTasks:     []*protocol.ReduceTask{},
			reduceTasksLeft: 0,
			expected:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := &protocol.Job{
				ID:     uuid.New().String(),
				Status: tt.jobStatus,
			}

			state := &JobState{
				mapTasks:        make([]*protocol.MapTask, 0),
				reduceTasks:     tt.reduceTasks,
				mapTasksLeft:    tt.mapTasksLeft,
				reduceTasksLeft: tt.reduceTasksLeft,
			}

			m := &Master{}
			result := m.isJobComplete(job, state)

			if result != tt.expected {
				t.Errorf("isJobComplete() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestCompleteJob(t *testing.T) {
	// Create a test master with storage
	m := &Master{
		jobs:      make(map[string]*protocol.Job),
		jobStates: make(map[string]*JobState),
		storage:   NewNoOpStorage(), // Use NoOp storage for testing
	}

	jobID := uuid.New().String()
	job := &protocol.Job{
		ID:        jobID,
		Status:    protocol.JobStatusRunning,
		StartedAt: time.Now().Add(-5 * time.Minute),
	}

	state := &JobState{
		mapTasks:        make([]*protocol.MapTask, 0),
		reduceTasks:     make([]*protocol.ReduceTask, 0),
		mapTasksLeft:    0,
		reduceTasksLeft: 0,
	}

	m.jobs[jobID] = job
	m.jobStates[jobID] = state
	m.currentJobID = jobID

	// Complete the job
	m.completeJob(jobID)

	// Verify job was marked as completed
	if job.Status != protocol.JobStatusCompleted {
		t.Errorf("Expected job status to be Completed, got %v", job.Status)
	}

	if job.CompletedAt.IsZero() {
		t.Error("Expected CompletedAt to be set")
	}

	// Verify current job was cleared
	if m.currentJobID != "" {
		t.Errorf("Expected currentJobID to be empty, got %s", m.currentJobID)
	}
}

func TestCheckJobCompletion(t *testing.T) {
	tests := []struct {
		name            string
		jobStatus       protocol.JobStatus
		mapTasksLeft    int
		reduceTasks     []*protocol.ReduceTask
		reduceTasksLeft int
		shouldComplete  bool
	}{
		{
			name:            "map-only job should complete",
			jobStatus:       protocol.JobStatusRunning,
			mapTasksLeft:    0,
			reduceTasks:     []*protocol.ReduceTask{},
			reduceTasksLeft: 0,
			shouldComplete:  true,
		},
		{
			name:            "map-reduce job should complete",
			jobStatus:       protocol.JobStatusRunning,
			mapTasksLeft:    0,
			reduceTasks:     []*protocol.ReduceTask{{ID: "reduce-1"}},
			reduceTasksLeft: 0,
			shouldComplete:  true,
		},
		{
			name:            "job with remaining tasks should not complete",
			jobStatus:       protocol.JobStatusRunning,
			mapTasksLeft:    1,
			reduceTasks:     []*protocol.ReduceTask{},
			reduceTasksLeft: 0,
			shouldComplete:  false,
		},
		{
			name:            "no current job should not complete",
			jobStatus:       protocol.JobStatusRunning,
			mapTasksLeft:    0,
			reduceTasks:     []*protocol.ReduceTask{},
			reduceTasksLeft: 0,
			shouldComplete:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Master{
				jobs:      make(map[string]*protocol.Job),
				jobStates: make(map[string]*JobState),
				storage:   NewNoOpStorage(),
			}

			jobID := uuid.New().String()
			job := &protocol.Job{
				ID:        jobID,
				Status:    tt.jobStatus,
				StartedAt: time.Now().Add(-5 * time.Minute),
			}

			state := &JobState{
				mapTasks:        make([]*protocol.MapTask, 0),
				reduceTasks:     tt.reduceTasks,
				mapTasksLeft:    tt.mapTasksLeft,
				reduceTasksLeft: tt.reduceTasksLeft,
			}

			m.jobs[jobID] = job
			m.jobStates[jobID] = state

			// Set current job only if we expect it to complete
			if tt.shouldComplete {
				m.currentJobID = jobID
			}

			// Check job completion
			m.checkJobCompletion()

			// Verify results
			if tt.shouldComplete {
				if job.Status != protocol.JobStatusCompleted {
					t.Errorf("Expected job status to be Completed, got %v", job.Status)
				}
				if m.currentJobID != "" {
					t.Errorf("Expected currentJobID to be empty, got %s", m.currentJobID)
				}
			} else {
				if job.Status == protocol.JobStatusCompleted {
					t.Error("Expected job status to not be Completed")
				}
			}
		})
	}
}

func TestResumeCurrentJob(t *testing.T) {
	tests := []struct {
		name            string
		jobStatus       protocol.JobStatus
		mapTasksLeft    int
		reduceTasks     []*protocol.ReduceTask
		reduceTasksLeft int
		shouldComplete  bool
	}{
		{
			name:            "map-only job should be marked complete on resume",
			jobStatus:       protocol.JobStatusRunning,
			mapTasksLeft:    0,
			reduceTasks:     []*protocol.ReduceTask{},
			reduceTasksLeft: 0,
			shouldComplete:  true,
		},
		{
			name:            "map-reduce job should be marked complete on resume",
			jobStatus:       protocol.JobStatusRunning,
			mapTasksLeft:    0,
			reduceTasks:     []*protocol.ReduceTask{{ID: "reduce-1"}},
			reduceTasksLeft: 0,
			shouldComplete:  true,
		},
		{
			name:            "job with remaining map tasks should not be marked complete",
			jobStatus:       protocol.JobStatusRunning,
			mapTasksLeft:    1,
			reduceTasks:     []*protocol.ReduceTask{},
			reduceTasksLeft: 0,
			shouldComplete:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Master{
				jobs:      make(map[string]*protocol.Job),
				jobStates: make(map[string]*JobState),
				storage:   NewNoOpStorage(),
			}

			jobID := uuid.New().String()
			job := &protocol.Job{
				ID:        jobID,
				Status:    tt.jobStatus,
				StartedAt: time.Now().Add(-5 * time.Minute),
			}

			state := &JobState{
				mapTasks:        make([]*protocol.MapTask, 0),
				reduceTasks:     tt.reduceTasks,
				mapTasksLeft:    tt.mapTasksLeft,
				reduceTasksLeft: tt.reduceTasksLeft,
			}

			m.jobs[jobID] = job
			m.jobStates[jobID] = state
			m.currentJobID = jobID

			// Resume the job
			err := m.resumeCurrentJob()
			if err != nil {
				t.Fatalf("resumeCurrentJob() failed: %v", err)
			}

			// Verify results
			if tt.shouldComplete {
				if job.Status != protocol.JobStatusCompleted {
					t.Errorf("Expected job status to be Completed, got %v", job.Status)
				}
				if m.currentJobID != "" {
					t.Errorf("Expected currentJobID to be empty, got %s", m.currentJobID)
				}
			} else {
				if job.Status == protocol.JobStatusCompleted {
					t.Error("Expected job status to not be Completed")
				}
				if m.currentJobID != jobID {
					t.Errorf("Expected currentJobID to remain %s, got %s", jobID, m.currentJobID)
				}
			}
		})
	}
}

func TestResumeCurrentJobWithInProgressTasks(t *testing.T) {
	m := &Master{
		jobs:      make(map[string]*protocol.Job),
		jobStates: make(map[string]*JobState),
		storage:   NewNoOpStorage(),
	}

	jobID := uuid.New().String()
	job := &protocol.Job{
		ID:        jobID,
		Status:    protocol.JobStatusRunning,
		StartedAt: time.Now().Add(-5 * time.Minute),
	}

	// Create tasks with some in progress
	mapTask1 := &protocol.MapTask{
		ID:       uuid.New().String(),
		Status:   protocol.TaskStatusCompleted,
		WorkerID: "",
	}
	mapTask2 := &protocol.MapTask{
		ID:       uuid.New().String(),
		Status:   protocol.TaskStatusInProgress,
		WorkerID: "worker-1",
	}
	reduceTask1 := &protocol.ReduceTask{
		ID:       uuid.New().String(),
		Status:   protocol.TaskStatusInProgress,
		WorkerID: "worker-2",
	}

	state := &JobState{
		mapTasks:        []*protocol.MapTask{mapTask1, mapTask2},
		reduceTasks:     []*protocol.ReduceTask{reduceTask1},
		mapTasksLeft:    1,
		reduceTasksLeft: 1,
	}

	m.jobs[jobID] = job
	m.jobStates[jobID] = state
	m.currentJobID = jobID

	// Resume the job
	err := m.resumeCurrentJob()
	if err != nil {
		t.Fatalf("resumeCurrentJob() failed: %v", err)
	}

	// Verify in-progress tasks were reset to idle
	if mapTask2.Status != protocol.TaskStatusIdle {
		t.Errorf("Expected map task to be reset to idle, got %v", mapTask2.Status)
	}
	if mapTask2.WorkerID != "" {
		t.Errorf("Expected map task worker ID to be cleared, got %s", mapTask2.WorkerID)
	}

	if reduceTask1.Status != protocol.TaskStatusIdle {
		t.Errorf("Expected reduce task to be reset to idle, got %v", reduceTask1.Status)
	}
	if reduceTask1.WorkerID != "" {
		t.Errorf("Expected reduce task worker ID to be cleared, got %s", reduceTask1.WorkerID)
	}

	// Verify task counts were recalculated
	if state.mapTasksLeft != 1 {
		t.Errorf("Expected mapTasksLeft to be 1, got %d", state.mapTasksLeft)
	}
	if state.reduceTasksLeft != 1 {
		t.Errorf("Expected reduceTasksLeft to be 1, got %d", state.reduceTasksLeft)
	}
}

func TestSubmitJob(t *testing.T) {
	m := &Master{
		jobs:      make(map[string]*protocol.Job),
		jobStates: make(map[string]*JobState),
		jobQueue:  []string{},
		storage:   NewNoOpStorage(),
	}

	// Test valid job submission with a file that exists
	req := protocol.JobSubmitRequest{
		Executor:    "wordcount",
		InputPath:   "../../../var/testdata.log", // Use existing test file
		OutputPath:  "/test/output.txt",
		ChunkSize:   1000,
		ReduceTasks: 3,
	}

	jobID, err := m.SubmitJob(req)
	if err != nil {
		t.Fatalf("SubmitJob() failed: %v", err)
	}

	if jobID == "" {
		t.Error("Expected non-empty job ID")
	}

	// Verify job was created
	job, exists := m.jobs[jobID]
	if !exists {
		t.Fatal("Job was not created")
	}

	if job.Status != protocol.JobStatusQueued {
		t.Errorf("Expected job status to be Queued, got %v", job.Status)
	}

	if job.Executor != req.Executor {
		t.Errorf("Expected executor to be %s, got %s", req.Executor, job.Executor)
	}

	// Verify job was added to queue
	if len(m.jobQueue) != 1 || m.jobQueue[0] != jobID {
		t.Errorf("Expected job to be in queue, got %v", m.jobQueue)
	}
}

func TestSubmitJobInvalidExecutor(t *testing.T) {
	m := &Master{
		jobs:      make(map[string]*protocol.Job),
		jobStates: make(map[string]*JobState),
		jobQueue:  []string{},
		storage:   NewNoOpStorage(),
	}

	req := protocol.JobSubmitRequest{
		Executor:    "invalid-executor",
		InputPath:   "/test/input.txt",
		OutputPath:  "/test/output.txt",
		ChunkSize:   1000,
		ReduceTasks: 3,
	}

	_, err := m.SubmitJob(req)
	if err == nil {
		t.Error("Expected error for invalid executor")
	}
}

func TestCancelJob(t *testing.T) {
	m := &Master{
		jobs:      make(map[string]*protocol.Job),
		jobStates: make(map[string]*JobState),
		jobQueue:  []string{},
		storage:   NewNoOpStorage(),
	}

	jobID := uuid.New().String()
	job := &protocol.Job{
		ID:     jobID,
		Status: protocol.JobStatusQueued,
	}

	m.jobs[jobID] = job
	m.jobQueue = append(m.jobQueue, jobID)

	// Cancel the job
	err := m.CancelJob(jobID)
	if err != nil {
		t.Fatalf("CancelJob() failed: %v", err)
	}

	// Verify job was cancelled
	if job.Status != protocol.JobStatusCancelled {
		t.Errorf("Expected job status to be Cancelled, got %v", job.Status)
	}

	// Verify job was removed from queue
	if len(m.jobQueue) != 0 {
		t.Errorf("Expected job to be removed from queue, got %v", m.jobQueue)
	}
}

func TestCancelJobAlreadyCompleted(t *testing.T) {
	m := &Master{
		jobs:      make(map[string]*protocol.Job),
		jobStates: make(map[string]*JobState),
		jobQueue:  []string{},
		storage:   NewNoOpStorage(),
	}

	jobID := uuid.New().String()
	job := &protocol.Job{
		ID:     jobID,
		Status: protocol.JobStatusCompleted,
	}

	m.jobs[jobID] = job

	// Try to cancel completed job
	err := m.CancelJob(jobID)
	if err == nil {
		t.Error("Expected error when cancelling completed job")
	}
}

func TestGetJob(t *testing.T) {
	m := &Master{
		jobs:      make(map[string]*protocol.Job),
		jobStates: make(map[string]*JobState),
		storage:   NewNoOpStorage(),
	}

	jobID := uuid.New().String()
	job := &protocol.Job{
		ID:     jobID,
		Status: protocol.JobStatusRunning,
	}

	m.jobs[jobID] = job

	// Get existing job
	retrievedJob := m.GetJob(jobID)
	if retrievedJob == nil {
		t.Fatal("Expected to retrieve job")
	}

	if retrievedJob.ID != jobID {
		t.Errorf("Expected job ID %s, got %s", jobID, retrievedJob.ID)
	}

	// Get non-existent job
	nonExistentJob := m.GetJob("non-existent")
	if nonExistentJob != nil {
		t.Error("Expected nil for non-existent job")
	}
}

func TestListJobs(t *testing.T) {
	m := &Master{
		jobs:      make(map[string]*protocol.Job),
		jobStates: make(map[string]*JobState),
		storage:   NewNoOpStorage(),
	}

	// Add some jobs
	job1 := &protocol.Job{ID: uuid.New().String(), Status: protocol.JobStatusQueued}
	job2 := &protocol.Job{ID: uuid.New().String(), Status: protocol.JobStatusRunning}
	job3 := &protocol.Job{ID: uuid.New().String(), Status: protocol.JobStatusCompleted}

	m.jobs[job1.ID] = job1
	m.jobs[job2.ID] = job2
	m.jobs[job3.ID] = job3

	// List jobs
	jobs := m.ListJobs()
	if len(jobs) != 3 {
		t.Errorf("Expected 3 jobs, got %d", len(jobs))
	}

	// Verify all jobs are present
	jobIDs := make(map[string]bool)
	for _, job := range jobs {
		jobIDs[job.ID] = true
	}

	if !jobIDs[job1.ID] || !jobIDs[job2.ID] || !jobIDs[job3.ID] {
		t.Error("Not all jobs were returned")
	}
}
