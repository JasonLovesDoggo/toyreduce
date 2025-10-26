package worker

import (
	"hash/fnv"
	"sort"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce"
)

// PartitionKey computes the partition for a key using FNV-1a hash
func PartitionKey(key string, numPartitions int) int {
	h := fnv.New32a()
	h.Write([]byte(key))

	return int(h.Sum32()) % numPartitions
}

// PartitionMapOutput groups key-value pairs by partition
func PartitionMapOutput(kvs []toyreduce.KeyValue, numPartitions int) map[int][]toyreduce.KeyValue {
	partitioned := make(map[int][]toyreduce.KeyValue)

	for _, kv := range kvs {
		partition := PartitionKey(kv.Key, numPartitions)
		partitioned[partition] = append(partitioned[partition], kv)
	}

	return partitioned
}

// ShuffleAndGroup sorts key-value pairs and groups by key
func ShuffleAndGroup(kvs []toyreduce.KeyValue) map[string][]string {
	// Sort by key
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})

	// Group by key
	grouped := make(map[string][]string)
	for _, kv := range kvs {
		grouped[kv.Key] = append(grouped[kv.Key], kv.Value)
	}

	return grouped
}
