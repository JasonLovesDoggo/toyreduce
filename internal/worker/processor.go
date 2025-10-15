package worker

import (
	"fmt"
	"log"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
)

// Processor handles map and reduce task execution
type Processor struct {
	worker toyreduce.Worker
	client *Client
}

// NewProcessor creates a new task processor
func NewProcessor(worker toyreduce.Worker, client *Client) *Processor {
	return &Processor{
		worker: worker,
		client: client,
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

	// Partition the output
	partitioned := PartitionMapOutput(emitted, task.NumPartitions)

	// Send each partition to cache
	for partition, kvs := range partitioned {
		if err := p.client.StoreMapOutput(task.ID, partition, kvs); err != nil {
			return fmt.Errorf("store partition %d error: %w", partition, err)
		}
		log.Printf("[WORKER:%s] Stored %d KVs for partition %d", workerID, len(kvs), partition)
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
	log.Printf("[WORKER:%s] Processing reduce task %s (partition %d)",
		workerID, task.ID, task.Partition)

	// Fetch intermediate data from cache
	intermediate, err := p.client.GetReduceInput(task.Partition)
	if err != nil {
		return fmt.Errorf("get reduce input error: %w", err)
	}

	log.Printf("[WORKER:%s] Retrieved %d intermediate KVs", workerID, len(intermediate))

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

	// Store results in cache
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
