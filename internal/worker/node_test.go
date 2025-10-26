package worker

import (
	"testing"

	"pkg.jsn.cam/toyreduce/pkg/executors"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
)

// TestGetExecutor verifies that executors.GetExecutor returns correct executors
func TestGetExecutor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		executor string
		wantNil  bool
	}{
		{
			name:     "wordcount executor exists",
			executor: "wordcount",
			wantNil:  false,
		},
		{
			name:     "actioncount executor exists",
			executor: "actioncount",
			wantNil:  false,
		},
		{
			name:     "maxvalue executor exists",
			executor: "maxvalue",
			wantNil:  false,
		},
		{
			name:     "urldedup executor exists",
			executor: "urldedup",
			wantNil:  false,
		},
		{
			name:     "average executor exists",
			executor: "average",
			wantNil:  false,
		},
		{
			name:     "unknown executor returns nil",
			executor: "nonexistent",
			wantNil:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			worker := executors.GetExecutor(tt.executor)
			if tt.wantNil && worker != nil {
				t.Errorf("executors.GetExecutor(%q) = %v, want nil", tt.executor, worker)
			}

			if !tt.wantNil && worker == nil {
				t.Errorf("executors.GetExecutor(%q) = nil, want non-nil", tt.executor)
			}
		})
	}
}

// TestDynamicExecutorHandling tests that workers can handle different executors dynamically
func TestDynamicExecutorHandling(t *testing.T) {
	t.Parallel()
	// Create a mock node with a processor
	node := &Node{
		id: "test-worker",
	}

	// Test that we can get different executors
	wordcountWorker := executors.GetExecutor("wordcount")
	actioncountWorker := executors.GetExecutor("actioncount")

	if wordcountWorker == nil {
		t.Fatal("wordcount executor not found")
	}

	if actioncountWorker == nil {
		t.Fatal("actioncount executor not found")
	}

	// Verify they are different types by checking their descriptions
	if wordcountWorker.Description() == actioncountWorker.Description() {
		t.Error("wordcount and actioncount should have different descriptions")
	}

	// Simulate processing tasks with different executors
	// First task with wordcount
	node.processor = NewProcessor(wordcountWorker, nil, nil)
	if node.processor.worker.Description() != wordcountWorker.Description() {
		t.Error("processor should use wordcount worker")
	}

	// Second task with actioncount - processor should be recreated
	node.processor = NewProcessor(actioncountWorker, nil, nil)
	if node.processor.worker.Description() != actioncountWorker.Description() {
		t.Error("processor should now use actioncount worker")
	}
}

// TestTaskExecutorField verifies that MapTask and ReduceTask have Executor field
func TestTaskExecutorField(t *testing.T) {
	t.Parallel()
	// Test MapTask has Executor field
	mapTask := &protocol.MapTask{
		ID:       "test-map-task",
		Executor: "wordcount",
		Chunk:    []string{"hello world"},
	}

	if mapTask.Executor != "wordcount" {
		t.Errorf("MapTask.Executor = %q, want %q", mapTask.Executor, "wordcount")
	}

	// Test ReduceTask has Executor field
	reduceTask := &protocol.ReduceTask{
		ID:        "test-reduce-task",
		Executor:  "actioncount",
		Partition: 0,
	}

	if reduceTask.Executor != "actioncount" {
		t.Errorf("ReduceTask.Executor = %q, want %q", reduceTask.Executor, "actioncount")
	}
}

// TestProcessorWorkerAccess tests that we can access the worker field in Processor
func TestProcessorWorkerAccess(t *testing.T) {
	t.Parallel()

	worker := executors.GetExecutor("wordcount")
	if worker == nil {
		t.Fatal("wordcount executor not found")
	}

	processor := NewProcessor(worker, nil, nil)

	// Test that processor.worker is accessible and correct
	if processor.worker == nil {
		t.Error("processor.worker should not be nil")
	}

	if processor.worker.Description() != worker.Description() {
		t.Errorf("processor.worker has wrong description: got %q, want %q",
			processor.worker.Description(), worker.Description())
	}
}

// TestMultipleExecutorSwitching simulates a worker handling multiple jobs with different executors
func TestMultipleExecutorSwitching(t *testing.T) {
	t.Parallel()

	node := &Node{
		id: "test-worker",
	}

	executorsList := []string{"wordcount", "actioncount", "maxvalue", "urldedup", "average"}

	for _, executorName := range executorsList {
		worker := executors.GetExecutor(executorName)
		if worker == nil {
			t.Fatalf("executor %q not found", executorName)
		}

		// Simulate switching executor
		if node.processor == nil || node.processor.worker != worker {
			node.processor = NewProcessor(worker, nil, nil)
		}

		// Verify the correct executor is active
		if node.processor.worker.Description() != worker.Description() {
			t.Errorf("Expected executor %q but got different worker", executorName)
		}
	}
}

// TestWorkerComparisonForExecutorChange tests the logic that determines if processor needs recreation
func TestWorkerComparisonForExecutorChange(t *testing.T) {
	t.Parallel()

	node := &Node{
		id: "test-worker",
	}

	// Start with wordcount
	worker1 := executors.GetExecutor("wordcount")
	node.processor = NewProcessor(worker1, nil, nil)

	// Different executor - should need recreation
	worker3 := executors.GetExecutor("actioncount")

	shouldRecreate := node.processor.worker != worker3
	if !shouldRecreate {
		t.Error("Worker instance should be different for different executor")
	}

	// Update processor with new worker
	node.processor = NewProcessor(worker3, nil, nil)
	if node.processor.worker == worker1 {
		t.Error("Processor should now use actioncount worker")
	}
}

// mockWorker is a simple test worker
type mockWorker struct {
	name string
}

func (w *mockWorker) Map(chunk []string, emit toyreduce.Emitter) error {
	return nil
}

func (w *mockWorker) Reduce(key string, values []string, emit toyreduce.Emitter) error {
	return nil
}

func (w *mockWorker) Description() string {
	return w.name
}

// TestProcessorRecreationWithMockWorkers tests processor recreation with mock workers
func TestProcessorRecreationWithMockWorkers(t *testing.T) {
	t.Parallel()

	node := &Node{
		id: "test-worker",
	}

	worker1 := &mockWorker{name: "mock-worker-1"}
	worker2 := &mockWorker{name: "mock-worker-2"}

	// Initial processor
	node.processor = NewProcessor(worker1, nil, nil)
	if node.processor.worker.Description() != "mock-worker-1" {
		t.Error("Initial processor should use mock-worker-1")
	}

	// Check if we need to recreate (we do, because it's a different worker)
	if node.processor.worker == worker2 {
		t.Error("worker1 and worker2 should be different instances")
	}

	// Recreate processor
	node.processor = NewProcessor(worker2, nil, nil)
	if node.processor.worker.Description() != "mock-worker-2" {
		t.Error("Updated processor should use mock-worker-2")
	}
}
