package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"go.etcd.io/bbolt"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce"
)

// Storage manages local intermediate data storage for a worker
type Storage struct {
	db   *bbolt.DB
	path string
}

// NewStorage creates a new worker storage instance
func NewStorage(dbPath string) (*Storage, error) {
	// Ensure directory exists
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create db directory: %w", err)
	}

	// Open database
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("open bbolt db: %w", err)
	}

	return &Storage{
		db:   db,
		path: dbPath,
	}, nil
}

// Close closes the database
func (s *Storage) Close() error {
	return s.db.Close()
}

// StorePartition stores intermediate data for a specific job and partition
func (s *Storage) StorePartition(jobID string, partition int, data []toyreduce.KeyValue) error {
	bucketName := []byte(fmt.Sprintf("job_%s_partition_%d", jobID, partition))

	return s.db.Update(func(tx *bbolt.Tx) error {
		// Create bucket if it doesn't exist
		b, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}

		// Get existing data if any
		var existing []toyreduce.KeyValue
		if v := b.Get([]byte("data")); v != nil {
			if err := json.Unmarshal(v, &existing); err != nil {
				return err
			}
		}

		// Append new data
		existing = append(existing, data...)

		// Store back
		encoded, err := json.Marshal(existing)
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

	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			// Bucket doesn't exist, return empty
			return nil
		}

		v := b.Get([]byte("data"))
		if v == nil {
			return nil
		}

		return json.Unmarshal(v, &result)
	})

	return result, err
}

// CleanupJob removes all partition data for a specific job
func (s *Storage) CleanupJob(jobID string) error {
	prefix := []byte(fmt.Sprintf("job_%s_", jobID))

	return s.db.Update(func(tx *bbolt.Tx) error {
		// Find and delete all buckets with this job prefix
		var bucketsToDelete [][]byte

		err := tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
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

	s.db.View(func(tx *bbolt.Tx) error {
		totalPartitions := 0
		totalKVs := 0

		tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			totalPartitions++

			v := b.Get([]byte("data"))
			if v != nil {
				var data []toyreduce.KeyValue
				if err := json.Unmarshal(v, &data); err == nil {
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
