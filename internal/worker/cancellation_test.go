package worker

import (
	"context"
	"testing"
	"time"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
)

// TestProcessMapTask_ContextTimeout verifies ProcessMapTask respects timeout
func TestProcessMapTask_ContextTimeout(t *testing.T) {
	t.Parallel()

	// Create a slow worker that delays on map
	slowWorker := &slowContextWorker{
		mapDelay: 500 * time.Millisecond,
	}

	processor := NewProcessor(slowWorker, nil, nil)

	task := &protocol.MapTask{
		ID:            "test-task",
		JobID:         "test-job",
		Chunk:         []string{"line1", "line2"},
		NumPartitions: 4,
	}

	// Create context with short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := processor.ProcessMapTask(ctx, task, "worker-1")

	// Should get timeout error
	if err == nil {
		t.Error("Expected error from timed-out map task")
	}
	if err != context.DeadlineExceeded && err.Error() != "map error: context deadline exceeded" {
		t.Logf("Got error: %v", err)
	}
	t.Logf("✓ ProcessMapTask returned error on timeout: %v", err)
}

// TestProcessMapTask_ContextCancellation verifies ProcessMapTask exits on cancellation
func TestProcessMapTask_ContextCancellation(t *testing.T) {
	t.Parallel()

	// Create a slow worker
	slowWorker := &slowContextWorker{
		mapDelay: 500 * time.Millisecond,
	}

	processor := NewProcessor(slowWorker, nil, nil)

	task := &protocol.MapTask{
		ID:            "test-task",
		JobID:         "test-job",
		Chunk:         []string{"line1", "line2"},
		NumPartitions: 4,
	}

	// Create context and cancel it
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)

	err := processor.ProcessMapTask(ctx, task, "worker-1")

	// Should get cancellation error
	if err == nil {
		t.Error("Expected error from cancelled map task")
	}
	t.Logf("✓ ProcessMapTask returned error on cancellation: %v", err)
}

// TestProcessMapTask_ImmediateCancellation verifies cancellation before start
func TestProcessMapTask_ImmediateCancellation(t *testing.T) {
	t.Parallel()

	worker := &simpleTestWorker{}
	processor := NewProcessor(worker, nil, nil)

	task := &protocol.MapTask{
		ID:            "test-task",
		JobID:         "test-job",
		Chunk:         []string{"hello", "world"},
		NumPartitions: 4,
	}

	// Create already-cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := processor.ProcessMapTask(ctx, task, "worker-1")

	// Should get cancellation error
	if err == nil {
		t.Error("Expected error from already-cancelled context")
	}
	if err != context.Canceled && err.Error() != "map error: context canceled" {
		t.Logf("Got error: %v", err)
	}
	t.Logf("✓ ProcessMapTask returned error on immediate cancellation: %v", err)
}

// slowContextWorker is a test worker that delays execution
type slowContextWorker struct {
	mapDelay time.Duration
}

func (w *slowContextWorker) Map(ctx context.Context, chunk []string, emit toyreduce.Emitter) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(w.mapDelay):
		for _, line := range chunk {
			emit(toyreduce.KeyValue{Key: line, Value: "1"})
		}
	}
	return nil
}

func (w *slowContextWorker) Reduce(ctx context.Context, key string, values []string, emit toyreduce.Emitter) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		emit(toyreduce.KeyValue{Key: key, Value: string(rune(len(values)))})
	}
	return nil
}

func (w *slowContextWorker) Description() string {
	return "slow context test worker"
}

// simpleTestWorker is a minimal test worker
type simpleTestWorker struct{}

func (w *simpleTestWorker) Map(ctx context.Context, chunk []string, emit toyreduce.Emitter) error {
	for _, line := range chunk {
		emit(toyreduce.KeyValue{Key: line, Value: "1"})
	}
	return nil
}

func (w *simpleTestWorker) Reduce(ctx context.Context, key string, values []string, emit toyreduce.Emitter) error {
	emit(toyreduce.KeyValue{Key: key, Value: string(rune(len(values)))})
	return nil
}

func (w *simpleTestWorker) Description() string {
	return "simple test worker"
}
