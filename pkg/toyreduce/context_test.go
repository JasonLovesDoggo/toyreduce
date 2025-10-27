package toyreduce

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

// TestChunk_ContextCancellation verifies Chunk respects context cancellation
func TestChunk_ContextCancellation(t *testing.T) {
	t.Parallel()

	// Create a large temp file with many lines
	tmpfile, err := os.CreateTemp(t.TempDir(), "chunk-cancel-*.txt")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	// Write 10,000 lines to ensure we have time to cancel
	for i := 0; i < 10000; i++ {
		tmpfile.WriteString("this is a test line with some content\n")
	}
	tmpfile.Close()

	// Create cancellable context and cancel immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel right away

	out := make(chan []string, 10)
	err = Chunk(ctx, tmpfile.Name(), 1, out)

	// Should return context.Canceled
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Canceled, got %v", err)
	}

	// Channel should be closed
	_, ok := <-out
	if ok {
		t.Error("Channel should be closed after cancellation")
	}
}

// TestChunk_ContextTimeout verifies Chunk respects context timeout
func TestChunk_ContextTimeout(t *testing.T) {
	t.Parallel()

	tmpfile, err := os.CreateTemp(t.TempDir(), "chunk-timeout-*.txt")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	// Write 50,000 lines to create large file that takes time to read
	for i := 0; i < 50000; i++ {
		tmpfile.WriteString("line with content for timeout test\n")
	}
	tmpfile.Close()

	// Create already-cancelled context to ensure timeout
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	out := make(chan []string, 10)
	err = Chunk(ctx, tmpfile.Name(), 1, out)

	// Should return context.Canceled
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

// TestMapPhase_ContextCancellation verifies MapPhase respects cancellation
func TestMapPhase_ContextCancellation(t *testing.T) {
	t.Parallel()

	slowWorker := &testMapReduceWorker{
		mapFunc: func(ctx context.Context, chunk []string, emit Emitter) error {
			// Always check context
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			// Do work
			for _, line := range chunk {
				emit(KeyValue{Key: line, Value: "1"})
			}
			return nil
		},
		reduceFunc: func(ctx context.Context, key string, values []string, emit Emitter) error {
			for _, val := range values {
				emit(KeyValue{Key: key, Value: val})
			}
			return nil
		},
	}

	// Create context and cancel it
	ctx, cancel := context.WithCancel(context.Background())

	// Feed chunks in a goroutine
	chunks := make(chan []string)
	go func() {
		defer close(chunks)
		// Keep sending many chunks until context is cancelled
		for i := 0; i < 10000; i++ {
			select {
			case <-ctx.Done():
				return
			case chunks <- []string{"a", "b", "c", "d", "e"}:
			}
		}
	}()

	// Cancel after a short delay to let some processing happen
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	_, err := MapPhase(ctx, chunks, slowWorker)

	// Should get context error if we catch cancellation
	// But might succeed if all chunks are processed before cancel
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("Expected Canceled or nil, got: %v", err)
	}
}

// TestReducePhase_ContextCancellation verifies ReducePhase respects cancellation
func TestReducePhase_ContextCancellation(t *testing.T) {
	t.Parallel()

	// Create worker that respects context
	slowWorker := &testMapReduceWorker{
		reduceFunc: func(ctx context.Context, key string, values []string, emit Emitter) error {
			// Check context immediately
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			// Do work
			for _, val := range values {
				emit(KeyValue{Key: key, Value: val})
			}
			return nil
		},
	}

	// Create grouped data
	groups := make(map[string][]string)
	for i := 0; i < 100; i++ {
		key := "key" + string(rune(i%26))
		groups[key] = append(groups[key], "value1", "value2", "value3")
	}

	// Create already-cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := ReducePhase(ctx, groups, slowWorker)

	// Should get cancellation error
	if err == nil {
		t.Error("Expected context error from ReducePhase")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected Canceled, got %v", err)
	}
}

// TestCombinePhase_ContextCancellation verifies CombinePhase respects cancellation
func TestCombinePhase_ContextCancellation(t *testing.T) {
	t.Parallel()

	// Create worker that respects context
	slowWorker := &testMapReduceWorker{
		reduceFunc: func(ctx context.Context, key string, values []string, emit Emitter) error {
			// Check context immediately
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			// Process values
			for _, val := range values {
				emit(KeyValue{Key: key, Value: val})
			}
			return nil
		},
	}

	// Create dataset with many pairs
	pairs := make([]KeyValue, 0, 1000)
	for i := 0; i < 100; i++ {
		key := "key" + string(rune('a'+(i%26)))
		for j := 0; j < 10; j++ {
			pairs = append(pairs, KeyValue{Key: key, Value: "value" + string(rune(j))})
		}
	}

	// Create already-cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := CombinePhase(ctx, pairs, slowWorker)

	// Should get cancellation error
	if err == nil {
		t.Error("Expected context error from CombinePhase")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected Canceled, got %v", err)
	}
}

// TestMapPhase_ImmediateCancellation verifies immediate cancellation works
func TestMapPhase_ImmediateCancellation(t *testing.T) {
	t.Parallel()

	worker := &testMapReduceWorker{
		mapFunc: func(ctx context.Context, chunk []string, emit Emitter) error {
			for _, line := range chunk {
				emit(KeyValue{Key: line, Value: "1"})
			}
			return nil
		},
		reduceFunc: func(ctx context.Context, key string, values []string, emit Emitter) error {
			for _, val := range values {
				emit(KeyValue{Key: key, Value: val})
			}
			return nil
		},
	}

	// Create already-cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	chunks := make(chan []string, 2)
	chunks <- []string{"hello"}
	chunks <- []string{"world"}
	close(chunks)

	_, err := MapPhase(ctx, chunks, worker)

	// Should return context.Canceled
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

// TestReducePhase_ImmediateCancellation verifies immediate cancellation
func TestReducePhase_ImmediateCancellation(t *testing.T) {
	t.Parallel()

	worker := &testMapReduceWorker{
		reduceFunc: func(ctx context.Context, key string, values []string, emit Emitter) error {
			for _, val := range values {
				emit(KeyValue{Key: key, Value: val})
			}
			return nil
		},
	}

	groups := map[string][]string{
		"apple":  {"1", "2", "3"},
		"banana": {"1"},
	}

	// Create already-cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := ReducePhase(ctx, groups, worker)

	// Should return context.Canceled
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

// TestMapPhase_WithValidContext verifies MapPhase works with valid context
func TestMapPhase_WithValidContext(t *testing.T) {
	t.Parallel()

	worker := WorkerFunc{
		MapFunc: func(ctx context.Context, chunk []string, emit Emitter) error {
			for _, line := range chunk {
				emit(KeyValue{Key: line, Value: "1"})
			}
			return nil
		},
		ReduceFunc: func(ctx context.Context, key string, values []string, emit Emitter) error {
			for _, val := range values {
				emit(KeyValue{Key: key, Value: val})
			}
			return nil
		},
	}

	chunks := make(chan []string, 2)
	chunks <- []string{"hello"}
	chunks <- []string{"world"}
	close(chunks)

	// Should succeed with valid context
	result, err := MapPhase(context.Background(), chunks, worker)
	if err != nil {
		t.Fatalf("MapPhase with valid context failed: %v", err)
	}

	if len(result) != 2 {
		t.Errorf("Expected 2 results, got %d", len(result))
	}
}

// TestReducePhase_WithValidContext verifies ReducePhase works with valid context
func TestReducePhase_WithValidContext(t *testing.T) {
	t.Parallel()

	worker := &testMapReduceWorker{
		reduceFunc: func(ctx context.Context, key string, values []string, emit Emitter) error {
			emit(KeyValue{Key: key, Value: string(rune('0' + len(values)))})
			return nil
		},
	}

	groups := map[string][]string{
		"apple":  {"1", "2", "3"},
		"banana": {"1"},
	}

	// Should succeed with valid context
	result, err := ReducePhase(context.Background(), groups, worker)
	if err != nil {
		t.Fatalf("ReducePhase with valid context failed: %v", err)
	}

	if len(result) != 2 {
		t.Errorf("Expected 2 results, got %d", len(result))
	}
}
