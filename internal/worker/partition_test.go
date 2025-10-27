package worker

import (
	"testing"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce"
)

func TestPartitionKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		key            string
		numPartitions  int
		wantConsistent bool // Same key should always give same partition
	}{
		{"basic", "hello", 4, true},
		{"single partition", "world", 1, true},
		{"large partition count", "test", 100, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Test consistency: same key should always give same partition
			partition1 := PartitionKey(tt.key, tt.numPartitions)
			partition2 := PartitionKey(tt.key, tt.numPartitions)

			if partition1 != partition2 {
				t.Errorf("PartitionKey not consistent: got %d and %d for same key", partition1, partition2)
			}

			// Test partition is in valid range
			if partition1 < 0 || partition1 >= tt.numPartitions {
				t.Errorf("PartitionKey(%q, %d) = %d, want value in range [0, %d)",
					tt.key, tt.numPartitions, partition1, tt.numPartitions)
			}
		})
	}
}

func TestPartitionKey_Distribution(t *testing.T) {
	t.Parallel()
	// Test that keys are distributed across partitions (not all in one)
	numPartitions := 4
	partitions := make(map[int]int)

	keys := []string{"apple", "banana", "cherry", "date", "elderberry", "fig", "grape", "honeydew"}
	for _, key := range keys {
		partition := PartitionKey(key, numPartitions)
		partitions[partition]++
	}

	// Should use at least 2 different partitions with 8 keys and 4 partitions
	if len(partitions) < 2 {
		t.Errorf("PartitionKey distributed %d keys into only %d partitions, expected at least 2",
			len(keys), len(partitions))
	}
}

func TestPartitionMapOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		kvs            []toyreduce.KeyValue
		numPartitions  int
		wantPartitions int // Number of partitions with data
	}{
		{
			name:           "empty input",
			kvs:            []toyreduce.KeyValue{},
			numPartitions:  4,
			wantPartitions: 0,
		},
		{
			name: "single key-value",
			kvs: []toyreduce.KeyValue{
				{Key: "hello", Value: "1"},
			},
			numPartitions:  4,
			wantPartitions: 1,
		},
		{
			name: "multiple keys",
			kvs: []toyreduce.KeyValue{
				{Key: "apple", Value: "1"},
				{Key: "banana", Value: "1"},
				{Key: "cherry", Value: "1"},
				{Key: "apple", Value: "2"}, // Duplicate key
			},
			numPartitions:  4,
			wantPartitions: -1, // Don't check exact count
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := PartitionMapOutput(tt.kvs, tt.numPartitions)

			// Check all partitions are in valid range
			for partition := range result {
				if partition < 0 || partition >= tt.numPartitions {
					t.Errorf("Got invalid partition %d, want range [0, %d)", partition, tt.numPartitions)
				}
			}

			// Check total count matches input
			totalKVs := 0
			for _, kvs := range result {
				totalKVs += len(kvs)
			}

			if totalKVs != len(tt.kvs) {
				t.Errorf("Total KVs in partitions = %d, want %d", totalKVs, len(tt.kvs))
			}

			// Check expected partition count if specified
			if tt.wantPartitions >= 0 && len(result) != tt.wantPartitions {
				t.Errorf("Got %d partitions with data, want %d", len(result), tt.wantPartitions)
			}

			// Verify same key always goes to same partition
			keyToPartition := make(map[string]int)

			for partition, kvs := range result {
				for _, kv := range kvs {
					if prevPartition, exists := keyToPartition[kv.Key]; exists {
						if prevPartition != partition {
							t.Errorf("Key %q found in multiple partitions: %d and %d",
								kv.Key, prevPartition, partition)
						}
					}

					keyToPartition[kv.Key] = partition
				}
			}
		})
	}
}

func TestShuffleAndGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want map[string][]string
		name string
		kvs  []toyreduce.KeyValue
	}{
		{
			name: "empty input",
			kvs:  []toyreduce.KeyValue{},
			want: map[string][]string{},
		},
		{
			name: "single key-value",
			kvs: []toyreduce.KeyValue{
				{Key: "hello", Value: "world"},
			},
			want: map[string][]string{
				"hello": {"world"},
			},
		},
		{
			name: "multiple values for same key",
			kvs: []toyreduce.KeyValue{
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
			kvs: []toyreduce.KeyValue{
				{Key: "banana", Value: "2"},
				{Key: "apple", Value: "1"},
				{Key: "cherry", Value: "3"},
				{Key: "apple", Value: "4"},
			},
			want: map[string][]string{
				"apple":  {"1", "4"},
				"banana": {"2"},
				"cherry": {"3"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := ShuffleAndGroup(tt.kvs)

			// Check all expected keys exist
			for key, wantValues := range tt.want {
				gotValues, exists := got[key]
				if !exists {
					t.Errorf("Key %q not found in result", key)
					continue
				}

				if len(gotValues) != len(wantValues) {
					t.Errorf("Key %q has %d values, want %d", key, len(gotValues), len(wantValues))
				}
				// Check values match (order matters since we sorted)
				for i, wantVal := range wantValues {
					if i >= len(gotValues) || gotValues[i] != wantVal {
						t.Errorf("Key %q value[%d] = %v, want %v", key, i, gotValues[i], wantVal)
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
