package store

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"

	pkgstorage "pkg.jsn.cam/toyreduce/pkg/storage"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce"
)

var (
	// Bucket names
	intermediateBucket = []byte("intermediate")
	resultsBucket      = []byte("results")
)

// Storage manages intermediate and final K-V data
type Storage struct {
	backend pkgstorage.Backend
	path    string
	counter atomic.Uint64 // Counter for append-only keys
}

// NewStorage creates a new storage instance
func NewStorage(dbPath string) (*Storage, error) {
	// Ensure directory exists
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create db directory: %w", err)
	}

	// Create backend
	backend, err := pkgstorage.NewBboltBackend(dbPath)
	if err != nil {
		return nil, fmt.Errorf("open backend: %w", err)
	}

	// Initialize buckets
	for _, bucket := range [][]byte{intermediateBucket, resultsBucket} {
		if err := backend.CreateBucket(bucket); err != nil {
			backend.Close()
			return nil, fmt.Errorf("create buckets: %w", err)
		}
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

// StoreMapOutput stores intermediate data for a specific partition
func (s *Storage) StoreMapOutput(partition int, data []toyreduce.KeyValue) error {
	key := []byte(fmt.Sprintf("partition_%d", partition))

	return s.backend.Update(func(tx pkgstorage.Transaction) error {
		b := tx.Bucket(intermediateBucket)
		if b == nil {
			return fmt.Errorf("bucket not found: %s", intermediateBucket)
		}

		// Get existing data
		var existing []toyreduce.KeyValue
		if v := b.Get(key); v != nil {
			if err := pkgstorage.DecodeJSON(v, &existing); err != nil {
				return err
			}
		}

		// Append new data
		existing = append(existing, data...)

		// Store back
		encoded, err := pkgstorage.EncodeJSON(existing)
		if err != nil {
			return err
		}

		return b.Put(key, encoded)
	})
}

// GetReduceInput retrieves all intermediate data for a partition
func (s *Storage) GetReduceInput(partition int) []toyreduce.KeyValue {
	key := []byte(fmt.Sprintf("partition_%d", partition))

	var result []toyreduce.KeyValue

	s.backend.View(func(tx pkgstorage.Transaction) error {
		b := tx.Bucket(intermediateBucket)
		if b == nil {
			return nil
		}

		if v := b.Get(key); v != nil {
			pkgstorage.DecodeJSON(v, &result)
		}

		return nil
	})

	return result
}

// StoreReduceOutput stores final reduce results with job ID
func (s *Storage) StoreReduceOutput(taskID, jobID string, data []toyreduce.KeyValue) error {
	key := []byte(fmt.Sprintf("job_%s_%s", jobID, taskID))

	return s.backend.Update(func(tx pkgstorage.Transaction) error {
		b := tx.Bucket(resultsBucket)
		if b == nil {
			return fmt.Errorf("bucket not found: %s", resultsBucket)
		}

		encoded, err := pkgstorage.EncodeJSON(data)
		if err != nil {
			return err
		}

		return b.Put(key, encoded)
	})
}

// GetReduceOutput retrieves reduce results for a task
func (s *Storage) GetReduceOutput(taskID string) ([]toyreduce.KeyValue, bool) {
	key := []byte(taskID)

	var (
		result []toyreduce.KeyValue
		found  bool
	)

	s.backend.View(func(tx pkgstorage.Transaction) error {
		b := tx.Bucket(resultsBucket)
		if b == nil {
			return nil
		}

		if v := b.Get(key); v != nil {
			pkgstorage.DecodeJSON(v, &result)

			found = true
		}

		return nil
	})

	return result, found
}

// GetJobResults returns all results for a specific job
func (s *Storage) GetJobResults(jobID string) []toyreduce.KeyValue {
	var all []toyreduce.KeyValue

	prefix := []byte(fmt.Sprintf("job_%s_", jobID))

	s.backend.View(func(tx pkgstorage.Transaction) error {
		b := tx.Bucket(resultsBucket)
		if b == nil {
			return nil
		}

		// Iterate over all keys and filter by prefix
		b.ForEach(func(k, v []byte) error {
			if bytes.HasPrefix(k, prefix) {
				var data []toyreduce.KeyValue
				if err := pkgstorage.DecodeJSON(v, &data); err != nil {
					return nil // Skip corrupted data
				}

				all = append(all, data...)
			}

			return nil
		})

		return nil
	})

	return all
}

// Reset clears all data (for reruns)
func (s *Storage) Reset() error {
	return s.backend.Update(func(tx pkgstorage.Transaction) error {
		// Delete and recreate buckets
		if err := tx.DeleteBucket(intermediateBucket); err != nil {
			return err
		}

		if err := tx.DeleteBucket(resultsBucket); err != nil {
			return err
		}

		if err := tx.CreateBucket(intermediateBucket); err != nil {
			return err
		}

		if err := tx.CreateBucket(resultsBucket); err != nil {
			return err
		}

		return nil
	})
}

// Stats returns storage statistics
func (s *Storage) Stats() map[string]interface{} {
	stats := make(map[string]interface{})

	s.backend.View(func(tx pkgstorage.Transaction) error {
		// Count intermediate partitions and KVs
		intermediatePartitions := 0
		intermediateKVs := 0

		b := tx.Bucket(intermediateBucket)
		if b != nil {
			b.ForEach(func(k, v []byte) error {
				intermediatePartitions++

				var data []toyreduce.KeyValue
				if err := pkgstorage.DecodeJSON(v, &data); err == nil {
					intermediateKVs += len(data)
				}

				return nil
			})
		}

		// Count reduce tasks and result KVs
		reduceTasks := 0
		resultKVs := 0

		b = tx.Bucket(resultsBucket)
		if b != nil {
			b.ForEach(func(k, v []byte) error {
				reduceTasks++

				var data []toyreduce.KeyValue
				if err := pkgstorage.DecodeJSON(v, &data); err == nil {
					resultKVs += len(data)
				}

				return nil
			})
		}

		stats["intermediate_partitions"] = intermediatePartitions
		stats["intermediate_kvs"] = intermediateKVs
		stats["reduce_tasks"] = reduceTasks
		stats["result_kvs"] = resultKVs
		stats["db_path"] = s.path
		stats["db_size_bytes"] = s.getFileSize()

		return nil
	})

	return stats
}

func (s *Storage) getFileSize() int64 {
	info, err := os.Stat(s.path)
	if err != nil {
		return 0
	}

	return info.Size()
}

// Compact forces a compaction of the database
func (s *Storage) Compact() error {
	// Create a temporary database
	tempPath := s.path + ".compact"

	tempBackend, err := pkgstorage.NewBboltBackend(tempPath)
	if err != nil {
		return err
	}

	// Create buckets in temp backend
	for _, bucket := range [][]byte{intermediateBucket, resultsBucket} {
		if err := tempBackend.CreateBucket(bucket); err != nil {
			tempBackend.Close()
			os.Remove(tempPath)

			return err
		}
	}

	// Copy data to temp database
	err = s.backend.View(func(srcTx pkgstorage.Transaction) error {
		return tempBackend.Update(func(dstTx pkgstorage.Transaction) error {
			// Copy intermediate bucket
			srcBucket := srcTx.Bucket(intermediateBucket)
			if srcBucket != nil {
				dstBucket := dstTx.Bucket(intermediateBucket)
				if dstBucket == nil {
					return fmt.Errorf("destination bucket not found: %s", intermediateBucket)
				}

				if err := srcBucket.ForEach(func(k, v []byte) error {
					return dstBucket.Put(k, v)
				}); err != nil {
					return err
				}
			}

			// Copy results bucket
			srcBucket = srcTx.Bucket(resultsBucket)
			if srcBucket != nil {
				dstBucket := dstTx.Bucket(resultsBucket)
				if dstBucket == nil {
					return fmt.Errorf("destination bucket not found: %s", resultsBucket)
				}

				if err := srcBucket.ForEach(func(k, v []byte) error {
					return dstBucket.Put(k, v)
				}); err != nil {
					return err
				}
			}

			return nil
		})
	})
	if err != nil {
		tempBackend.Close()
		os.Remove(tempPath)

		return err
	}

	tempBackend.Close()

	// Close original backend
	s.backend.Close()

	// Replace with compacted version
	if err := os.Rename(tempPath, s.path); err != nil {
		return err
	}

	// Reopen backend
	backend, err := pkgstorage.NewBboltBackend(s.path)
	if err != nil {
		return err
	}

	s.backend = backend

	return nil
}

// partitionKey creates a key for partition data
func partitionKey(partition int) []byte {
	return []byte(strconv.Itoa(partition))
}
