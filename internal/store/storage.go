package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"

	"go.etcd.io/bbolt"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce"
)

var (
	// Bucket names
	intermediateBucket = []byte("intermediate")
	resultsBucket      = []byte("results")
)

// Storage manages intermediate and final K-V data using bbolt
type Storage struct {
	db      *bbolt.DB
	path    string
	counter atomic.Uint64 // Counter for append-only keys
}

// NewStorage creates a new bbolt-backed storage instance
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

	// Initialize buckets
	err = db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(intermediateBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(resultsBucket); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("create buckets: %w", err)
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

// StoreMapOutput stores intermediate data for a specific partition
func (s *Storage) StoreMapOutput(partition int, data []toyreduce.KeyValue) error {
	key := []byte(fmt.Sprintf("partition_%d", partition))

	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(intermediateBucket)

		// Get existing data
		var existing []toyreduce.KeyValue
		if v := b.Get(key); v != nil {
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

		return b.Put(key, encoded)
	})
}

// GetReduceInput retrieves all intermediate data for a partition
func (s *Storage) GetReduceInput(partition int) []toyreduce.KeyValue {
	key := []byte(fmt.Sprintf("partition_%d", partition))

	var result []toyreduce.KeyValue

	s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(intermediateBucket)
		if v := b.Get(key); v != nil {
			json.Unmarshal(v, &result)
		}
		return nil
	})

	return result
}

// StoreReduceOutput stores final reduce results with job ID
func (s *Storage) StoreReduceOutput(taskID, jobID string, data []toyreduce.KeyValue) error {
	key := []byte(fmt.Sprintf("job_%s_%s", jobID, taskID))

	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(resultsBucket)

		encoded, err := json.Marshal(data)
		if err != nil {
			return err
		}

		return b.Put(key, encoded)
	})
}

// GetReduceOutput retrieves reduce results for a task
func (s *Storage) GetReduceOutput(taskID string) ([]toyreduce.KeyValue, bool) {
	key := []byte(taskID)

	var result []toyreduce.KeyValue
	var found bool

	s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(resultsBucket)
		if v := b.Get(key); v != nil {
			json.Unmarshal(v, &result)
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

	s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(resultsBucket)
		c := b.Cursor()

		// Iterate over keys with the job prefix
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			var data []toyreduce.KeyValue
			if err := json.Unmarshal(v, &data); err != nil {
				continue
			}
			all = append(all, data...)
		}
		return nil
	})

	return all
}

// Reset clears all data (for reruns)
func (s *Storage) Reset() error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		// Delete and recreate buckets
		if err := tx.DeleteBucket(intermediateBucket); err != nil && err != bbolt.ErrBucketNotFound {
			return err
		}
		if err := tx.DeleteBucket(resultsBucket); err != nil && err != bbolt.ErrBucketNotFound {
			return err
		}

		if _, err := tx.CreateBucket(intermediateBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucket(resultsBucket); err != nil {
			return err
		}

		return nil
	})
}

// Stats returns storage statistics
func (s *Storage) Stats() map[string]interface{} {
	stats := make(map[string]interface{})

	s.db.View(func(tx *bbolt.Tx) error {
		// Count intermediate partitions and KVs
		intermediatePartitions := 0
		intermediateKVs := 0

		b := tx.Bucket(intermediateBucket)
		b.ForEach(func(k, v []byte) error {
			intermediatePartitions++
			var data []toyreduce.KeyValue
			if err := json.Unmarshal(v, &data); err == nil {
				intermediateKVs += len(data)
			}
			return nil
		})

		// Count reduce tasks and result KVs
		reduceTasks := 0
		resultKVs := 0

		b = tx.Bucket(resultsBucket)
		b.ForEach(func(k, v []byte) error {
			reduceTasks++
			var data []toyreduce.KeyValue
			if err := json.Unmarshal(v, &data); err == nil {
				resultKVs += len(data)
			}
			return nil
		})

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

	tempDB, err := bbolt.Open(tempPath, 0600, nil)
	if err != nil {
		return err
	}

	// Copy data to temp database
	err = s.db.View(func(srcTx *bbolt.Tx) error {
		return tempDB.Update(func(dstTx *bbolt.Tx) error {
			// Copy intermediate bucket
			srcBucket := srcTx.Bucket(intermediateBucket)
			if srcBucket != nil {
				dstBucket, err := dstTx.CreateBucket(intermediateBucket)
				if err != nil {
					return err
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
				dstBucket, err := dstTx.CreateBucket(resultsBucket)
				if err != nil {
					return err
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
		tempDB.Close()
		os.Remove(tempPath)
		return err
	}

	tempDB.Close()

	// Close original database
	s.db.Close()

	// Replace with compacted version
	if err := os.Rename(tempPath, s.path); err != nil {
		return err
	}

	// Reopen database
	db, err := bbolt.Open(s.path, 0600, nil)
	if err != nil {
		return err
	}
	s.db = db

	return nil
}

// partitionKey creates a key for partition data
func partitionKey(partition int) []byte {
	return []byte(strconv.Itoa(partition))
}
