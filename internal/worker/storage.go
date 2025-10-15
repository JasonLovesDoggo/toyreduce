package worker

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"pkg.jsn.cam/toyreduce/pkg/storage"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce"
)

// Storage manages local intermediate data storage for a worker
type Storage struct {
	backend storage.Backend
	path    string
}

// NewStorage creates a new worker storage instance
func NewStorage(dbPath string) (*Storage, error) {
	// Ensure directory exists
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create db directory: %w", err)
	}

	// Create backend
	backend, err := storage.NewBboltBackend(dbPath)
	if err != nil {
		return nil, fmt.Errorf("open backend: %w", err)
	}

	return &Storage{
		backend: backend,
		path:    dbPath,
	}, nil
}

// Close closes the database
func (s *Storage) Close() error {
	return s.backend.Close()
}

// StorePartition stores intermediate data for a specific job and partition
func (s *Storage) StorePartition(jobID string, partition int, data []toyreduce.KeyValue) error {
	bucketName := []byte(fmt.Sprintf("job_%s_partition_%d", jobID, partition))

	return s.backend.Update(func(tx storage.Transaction) error {
		// Create bucket if it doesn't exist
		if err := tx.CreateBucket(bucketName); err != nil {
			return err
		}

		b := tx.Bucket(bucketName)
		if b == nil {
			return fmt.Errorf("bucket not found: %s", bucketName)
		}

		// Get existing data if any
		var existing []toyreduce.KeyValue
		if v := b.Get([]byte("data")); v != nil {
			if err := storage.DecodeJSON(v, &existing); err != nil {
				return err
			}
		}

		// Append new data
		existing = append(existing, data...)

		// Store back
		encoded, err := storage.EncodeJSON(existing)
		if err != nil {
			return err
		}

		return b.Put([]byte("data"), encoded)
	})
}

// GetPartition retrieves all data for a specific job and partition
func (s *Storage) GetPartition(jobID string, partition int) ([]toyreduce.KeyValue, error) {
	bucketName := []byte(fmt.Sprintf("job_%s_partition_%d", jobID, partition))

	var result []toyreduce.KeyValue

	err := s.backend.View(func(tx storage.Transaction) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			// Bucket doesn't exist, return empty
			return nil
		}

		v := b.Get([]byte("data"))
		if v == nil {
			return nil
		}

		return storage.DecodeJSON(v, &result)
	})

	return result, err
}

// CleanupJob removes all partition data for a specific job
func (s *Storage) CleanupJob(jobID string) error {
	prefix := []byte(fmt.Sprintf("job_%s_", jobID))

	return s.backend.Update(func(tx storage.Transaction) error {
		// Find and delete all buckets with this job prefix
		var bucketsToDelete [][]byte

		err := tx.ForEachBucket(func(name []byte) error {
			if bytes.HasPrefix(name, prefix) {
				bucketsToDelete = append(bucketsToDelete, append([]byte(nil), name...))
			}
			return nil
		})
		if err != nil {
			return err
		}

		// Delete all matching buckets
		for _, name := range bucketsToDelete {
			if err := tx.DeleteBucket(name); err != nil {
				return err
			}
		}

		return nil
	})
}

// Stats returns storage statistics
func (s *Storage) Stats() map[string]interface{} {
	stats := make(map[string]interface{})

	s.backend.View(func(tx storage.Transaction) error {
		totalPartitions := 0
		totalKVs := 0

		tx.ForEachBucket(func(name []byte) error {
			totalPartitions++

			b := tx.Bucket(name)
			if b == nil {
				return nil
			}

			v := b.Get([]byte("data"))
			if v != nil {
				var data []toyreduce.KeyValue
				if err := storage.DecodeJSON(v, &data); err == nil {
					totalKVs += len(data)
				}
			}

			return nil
		})

		stats["total_partitions"] = totalPartitions
		stats["total_kvs"] = totalKVs
		stats["db_path"] = s.path

		return nil
	})

	return stats
}
