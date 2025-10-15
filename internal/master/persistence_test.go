package master

import (
	"testing"

	"pkg.jsn.cam/toyreduce/pkg/storage"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
)

// TestRestoreQueueIgnoresStaleStorage verifies that completed jobs
// are not restarted after master restart, even if the persisted queue
// contains stale data with completed job IDs.
func TestRestoreQueueIgnoresStaleStorage(t *testing.T) {
	// Setup: Use MemoryBackend and pre-populate it
	backend := storage.NewMemoryBackend()
	masterStorage, err := NewMasterStorage(backend)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	// Pre-populate with test data
	jobs := map[string]*protocol.Job{
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
	}
	for _, job := range jobs {
		masterStorage.SaveJob(job)
	}
	masterStorage.SaveQueue([]string{"completed-job", "queued-job"}) // STALE QUEUE
	masterStorage.SaveCurrentJobID("fake-running-job")               // fake current job

	m := &Master{
		jobs:      make(map[string]*protocol.Job),
		jobStates: make(map[string]*JobState),
		storage:   masterStorage,
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
