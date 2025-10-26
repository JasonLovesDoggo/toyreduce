package toyreduce

import (
	"os"
	"testing"
)

func TestChunk(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		fileContent    string
		chunkSizeMB    int
		wantChunks     int
		wantTotalLines int
	}{
		{
			name:           "empty file",
			fileContent:    "",
			chunkSizeMB:    1,
			wantChunks:     0,
			wantTotalLines: 0,
		},
		{
			name:           "single line",
			fileContent:    "hello world",
			chunkSizeMB:    1,
			wantChunks:     1,
			wantTotalLines: 1,
		},
		{
			name:           "multiple small lines",
			fileContent:    "line1\nline2\nline3\n",
			chunkSizeMB:    1,
			wantChunks:     1,
			wantTotalLines: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Create temp file
			tmpfile, err := os.CreateTemp(t.TempDir(), "chunk-test-*.txt")
			if err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}
			defer os.Remove(tmpfile.Name())

			if _, err := tmpfile.WriteString(tt.fileContent); err != nil {
				t.Fatalf("Failed to write temp file: %v", err)
			}

			tmpfile.Close()

			// Run Chunk
			out := make(chan []string, 10)
			errCh := make(chan error, 1)

			go func() {
				errCh <- Chunk(tmpfile.Name(), tt.chunkSizeMB, out)
			}()

			// Collect chunks
			var chunks [][]string
			for chunk := range out {
				chunks = append(chunks, chunk)
			}

			// Check error
			if err := <-errCh; err != nil {
				t.Fatalf("Chunk() returned error: %v", err)
			}

			// Verify chunk count
			if len(chunks) != tt.wantChunks {
				t.Errorf("Got %d chunks, want %d", len(chunks), tt.wantChunks)
			}

			// Verify total lines
			totalLines := 0
			for _, chunk := range chunks {
				totalLines += len(chunk)
			}

			if totalLines != tt.wantTotalLines {
				t.Errorf("Got %d total lines, want %d", totalLines, tt.wantTotalLines)
			}
		})
	}
}

func TestChunk_LargeFile(t *testing.T) {
	t.Parallel()
	// Create a file with lines that will span multiple chunks
	tmpfile, err := os.CreateTemp(t.TempDir(), "chunk-large-*.txt")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	// Write 1000 lines (approximately 50KB if each line is ~50 bytes)
	numLines := 1000
	for range numLines {
		tmpfile.WriteString("This is line number and it contains some text\n")
	}

	tmpfile.Close()

	// Chunk with small chunk size to force multiple chunks
	// 50KB / 10KB = ~5 chunks expected
	out := make(chan []string, 10)
	chunkSizeMB := 1 // 1MB chunk size

	errCh := make(chan error, 1)

	go func() {
		errCh <- Chunk(tmpfile.Name(), chunkSizeMB, out)
	}()

	var chunks [][]string
	for chunk := range out {
		chunks = append(chunks, chunk)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("Chunk() returned error: %v", err)
	}

	// Verify all lines are present
	totalLines := 0
	for _, chunk := range chunks {
		totalLines += len(chunk)
	}

	if totalLines != numLines {
		t.Errorf("Got %d total lines, want %d", totalLines, numLines)
	}

	// All chunks should be non-empty
	for i, chunk := range chunks {
		if len(chunk) == 0 {
			t.Errorf("Chunk %d is empty", i)
		}
	}
}

func TestChunk_NonexistentFile(t *testing.T) {
	t.Parallel()

	out := make(chan []string, 1)

	err := Chunk("/nonexistent/path/file.txt", 1, out)
	if err == nil {
		t.Error("Expected error for nonexistent file, got nil")
	}

	// Channel should be closed
	_, ok := <-out
	if ok {
		t.Error("Expected channel to be closed after error")
	}
}

func TestShuffle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		pairs []KeyValue
		want  map[string][]string
	}{
		{
			name:  "empty input",
			pairs: []KeyValue{},
			want:  map[string][]string{},
		},
		{
			name: "single pair",
			pairs: []KeyValue{
				{Key: "hello", Value: "world"},
			},
			want: map[string][]string{
				"hello": {"world"},
			},
		},
		{
			name: "multiple values same key",
			pairs: []KeyValue{
				{Key: "word", Value: "1"},
				{Key: "word", Value: "2"},
				{Key: "word", Value: "3"},
			},
			want: map[string][]string{
				"word": {"1", "2", "3"},
			},
		},
		{
			name: "multiple keys",
			pairs: []KeyValue{
				{Key: "apple", Value: "1"},
				{Key: "banana", Value: "2"},
				{Key: "apple", Value: "3"},
				{Key: "cherry", Value: "4"},
			},
			want: map[string][]string{
				"apple":  {"1", "3"},
				"banana": {"2"},
				"cherry": {"4"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := Shuffle(tt.pairs)

			// Check all expected keys
			for key, wantValues := range tt.want {
				gotValues, exists := got[key]
				if !exists {
					t.Errorf("Key %q not found in result", key)
					continue
				}

				if len(gotValues) != len(wantValues) {
					t.Errorf("Key %q has %d values, want %d", key, len(gotValues), len(wantValues))
					continue
				}
				// Check values match (order matters)
				for i, wantVal := range wantValues {
					if gotValues[i] != wantVal {
						t.Errorf("Key %q value[%d] = %q, want %q", key, i, gotValues[i], wantVal)
					}
				}
			}

			// Check no unexpected keys
			if len(got) != len(tt.want) {
				t.Errorf("Got %d keys, want %d", len(got), len(tt.want))
			}
		})
	}
}

func TestMapPhase(t *testing.T) {
	t.Parallel()
	t.Run("simple map", func(t *testing.T) {
		// Worker that emits key-value pairs and has a passthrough reduce
		var myWorker Worker = WorkerFunc{
			MapFunc: func(chunk []string, emit Emitter) error {
				for _, line := range chunk {
					emit(KeyValue{Key: line, Value: "1"})
				}

				return nil
			},
			ReduceFunc: func(key string, values []string, emit Emitter) error {
				// Passthrough for testing
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

		result, err := MapPhase(chunks, myWorker)
		if err != nil {
			t.Fatalf("MapPhase error: %v", err)
		}

		if len(result) != 2 {
			t.Fatalf("Got %d results, want 2. Results: %+v", len(result), result)
		}

		// Check that we got both keys
		keys := make(map[string]bool)
		for _, kv := range result {
			keys[kv.Key] = true
		}

		if !keys["hello"] || !keys["world"] {
			t.Errorf("Missing expected keys. Got: %v", keys)
		}
	})
}

// WorkerFunc allows using functions as Workers
type WorkerFunc struct {
	MapFunc    func([]string, Emitter) error
	ReduceFunc func(string, []string, Emitter) error
}

func (w WorkerFunc) Map(chunk []string, emit Emitter) error {
	if w.MapFunc != nil {
		return w.MapFunc(chunk, emit)
	}

	return nil
}

func (w WorkerFunc) Reduce(key string, values []string, emit Emitter) error {
	if w.ReduceFunc != nil {
		return w.ReduceFunc(key, values, emit)
	}

	return nil
}

func (w WorkerFunc) Description() string {
	return "test worker func"
}

func TestReducePhase(t *testing.T) {
	t.Parallel()
	// Create a test worker that counts values
	testWorker := &testMapReduceWorker{
		reduceFunc: func(key string, values []string, emit Emitter) error {
			// Sum the values
			total := len(values)
			emit(KeyValue{Key: key, Value: string(rune('0' + total))})

			return nil
		},
	}

	groups := map[string][]string{
		"apple":  {"1", "2", "3"},
		"banana": {"1"},
		"cherry": {"1", "2"},
	}

	result := ReducePhase(groups, testWorker)

	// Should have 3 results (one per key)
	if len(result) != 3 {
		t.Errorf("Got %d results, want 3", len(result))
	}

	// Check results exist for all keys
	resultKeys := make(map[string]string)
	for _, kv := range result {
		resultKeys[kv.Key] = kv.Value
	}

	wantResults := map[string]string{
		"apple":  "3", // 3 values
		"banana": "1", // 1 value
		"cherry": "2", // 2 values
	}

	for key, wantVal := range wantResults {
		if gotVal, exists := resultKeys[key]; !exists {
			t.Errorf("Key %q not found in results", key)
		} else if gotVal != wantVal {
			t.Errorf("Key %q value = %q, want %q", key, gotVal, wantVal)
		}
	}
}

// Helper test worker implementation
type testMapReduceWorker struct {
	mapFunc    func(chunk []string, emit Emitter) error
	reduceFunc func(key string, values []string, emit Emitter) error
}

func (w *testMapReduceWorker) Map(chunk []string, emit Emitter) error {
	if w.mapFunc != nil {
		return w.mapFunc(chunk, emit)
	}

	return nil
}

func (w *testMapReduceWorker) Reduce(key string, values []string, emit Emitter) error {
	if w.reduceFunc != nil {
		return w.reduceFunc(key, values, emit)
	}

	return nil
}

func (w *testMapReduceWorker) Description() string {
	return "test worker"
}
