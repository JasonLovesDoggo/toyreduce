package master

import (
	"testing"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
)

// TestRestoreQueueIgnoresStaleStorage verifies that completed jobs
// are not restarted after master restart, even if the persisted queue
// contains stale data with completed job IDs.
func TestRestoreQueueIgnoresStaleStorage(t *testing.T) {
	// Setup: Create storage with a completed job + a queued job
	storage := &MockStorage{
		jobs: map[string]*protocol.Job{
			"completed-job": {
				ID:       "completed-job",
				Status:   protocol.JobStatusCompleted,
				Executor: "wordcount",
			},
			"queued-job": {
				ID:       "queued-job",
				Status:   protocol.JobStatusQueued,
				Executor: "wordcount",
			},
		},
		// STALE QUEUE: Contains a completed job (simulating the bug scenario)
		queue: []string{"completed-job", "queued-job"},
		// Set a fake current job to prevent auto-start during restore
		jobID: "fake-running-job",
	}

	m := &Master{
		jobs:      make(map[string]*protocol.Job),
		jobStates: make(map[string]*JobState),
		storage:   storage,
	}

	// Restore state (calls the actual restore logic)
	if err := m.restore(); err != nil {
		t.Fatalf("restore() failed: %v", err)
	}

	// Clear the fake current job for verification
	m.currentJobID = ""

	// Verify: Only queued job should be in the queue
	if len(m.jobQueue) != 1 {
		t.Errorf("Expected 1 job in queue, got %d: %v", len(m.jobQueue), m.jobQueue)
	}

	if len(m.jobQueue) > 0 && m.jobQueue[0] != "queued-job" {
		t.Errorf("Expected queued-job in queue, got %s", m.jobQueue[0])
	}

	// Verify: Completed job must NOT be in queue
	for _, id := range m.jobQueue {
		if id == "completed-job" {
			t.Fatal("BUG: Completed job should not be in queue after restore")
		}
	}
}

// MockStorage implements Storage interface for testing
type MockStorage struct {
	jobs   map[string]*protocol.Job
	states map[string]*JobState
	queue  []string
	jobID  string
}

func (m *MockStorage) SaveJob(job *protocol.Job) error {
	m.jobs[job.ID] = job
	return nil
}

func (m *MockStorage) LoadJobs() (map[string]*protocol.Job, error) {
	return m.jobs, nil
}

func (m *MockStorage) DeleteJob(jobID string) error {
	delete(m.jobs, jobID)
	return nil
}

func (m *MockStorage) SaveJobState(jobID string, state *JobState) error {
	if m.states == nil {
		m.states = make(map[string]*JobState)
	}
	m.states[jobID] = state
	return nil
}

func (m *MockStorage) LoadJobStates() (map[string]*JobState, error) {
	if m.states == nil {
		return make(map[string]*JobState), nil
	}
	return m.states, nil
}

func (m *MockStorage) DeleteJobState(jobID string) error {
	delete(m.states, jobID)
	return nil
}

func (m *MockStorage) SaveQueue(queue []string) error {
	m.queue = queue
	return nil
}

func (m *MockStorage) LoadQueue() ([]string, error) {
	// Return the stale queue (simulating the bug scenario)
	return m.queue, nil
}

func (m *MockStorage) SaveCurrentJobID(jobID string) error {
	m.jobID = jobID
	return nil
}

func (m *MockStorage) LoadCurrentJobID() (string, error) {
	return m.jobID, nil
}

func (m *MockStorage) Close() error {
	return nil
}
