package worker

import (
	"fmt"
	"log"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
)

// Processor handles map and reduce task execution
type Processor struct {
	worker  toyreduce.Worker
	client  *Client
	storage *Storage
}

// NewProcessor creates a new task processor
func NewProcessor(worker toyreduce.Worker, client *Client, storage *Storage) *Processor {
	return &Processor{
		worker:  worker,
		client:  client,
		storage: storage,
	}
}

// ProcessMapTask executes a map task
func (p *Processor) ProcessMapTask(task *protocol.MapTask, workerID string) error {
	log.Printf("[WORKER:%s] Processing map task %s (%d lines)",
		workerID, task.ID, len(task.Chunk))

	// Collect emitted key-value pairs
	var emitted []toyreduce.KeyValue
	emitter := func(kv toyreduce.KeyValue) {
		emitted = append(emitted, kv)
	}

	// Execute map function
	if err := p.worker.Map(task.Chunk, emitter); err != nil {
		return fmt.Errorf("map error: %w", err)
	}

	log.Printf("[WORKER:%s] Map emitted %d key-value pairs", workerID, len(emitted))

	// Apply combine phase to reduce intermediate data before partitioning
	combined := toyreduce.CombinePhase(emitted, p.worker)
	log.Printf("[WORKER:%s] Combine reduced to %d key-value pairs", workerID, len(combined))

	// Partition the output
	partitioned := PartitionMapOutput(combined, task.NumPartitions)

	// Store each partition locally
	for partition, kvs := range partitioned {
		if err := p.storage.StorePartition(task.JobID, partition, kvs); err != nil {
			return fmt.Errorf("store partition %d error: %w", partition, err)
		}
		log.Printf("[WORKER:%s] Stored %d KVs locally for partition %d", workerID, len(kvs), partition)
	}

	// Notify master of completion
	if err := p.client.CompleteTask(task.ID, workerID, task.Version, true, ""); err != nil {
		return fmt.Errorf("complete task error: %w", err)
	}

	log.Printf("[WORKER:%s] Map task %s completed successfully", workerID, task.ID)
	return nil
}

// ProcessReduceTask executes a reduce task
func (p *Processor) ProcessReduceTask(task *protocol.ReduceTask, workerID string) error {
	log.Printf("[WORKER:%s] Processing reduce task %s (partition %d) from %d workers",
		workerID, task.ID, task.Partition, len(task.WorkerEndpoints))

	// Fetch partition data from all map workers
	var intermediate []toyreduce.KeyValue
	for _, endpoint := range task.WorkerEndpoints {
		data, err := p.client.FetchPartitionFromWorker(endpoint, task.JobID, task.Partition)
		if err != nil {
			log.Printf("[WORKER:%s] Warning: Failed to fetch from %s: %v", workerID, endpoint, err)
			// Continue with other workers - partial data is better than failure
			continue
		}
		intermediate = append(intermediate, data...)
		log.Printf("[WORKER:%s] Fetched %d KVs from %s", workerID, len(data), endpoint)
	}

	log.Printf("[WORKER:%s] Retrieved %d total intermediate KVs from %d workers",
		workerID, len(intermediate), len(task.WorkerEndpoints))

	// Shuffle and group by key
	grouped := ShuffleAndGroup(intermediate)

	log.Printf("[WORKER:%s] Grouped into %d unique keys", workerID, len(grouped))

	// Collect reduce outputs
	var results []toyreduce.KeyValue
	emitter := func(kv toyreduce.KeyValue) {
		results = append(results, kv)
	}

	// Execute reduce function for each key
	for key, values := range grouped {
		if err := p.worker.Reduce(key, values, emitter); err != nil {
			return fmt.Errorf("reduce error for key %s: %w", key, err)
		}
	}

	log.Printf("[WORKER:%s] Reduce produced %d results", workerID, len(results))

	// Store results in store
	if err := p.client.StoreReduceOutput(task.ID, task.JobID, results); err != nil {
		return fmt.Errorf("store reduce output error: %w", err)
	}

	// Notify master of completion
	if err := p.client.CompleteTask(task.ID, workerID, task.Version, true, ""); err != nil {
		return fmt.Errorf("complete task error: %w", err)
	}

	log.Printf("[WORKER:%s] Reduce task %s completed successfully", workerID, task.ID)
	return nil
}
