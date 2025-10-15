package master

import (
	"fmt"
	"log"

	bolt "go.etcd.io/bbolt"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
)

var (
	jobsBucket      = []byte("jobs")
	jobStatesBucket = []byte("job_states")
	queueBucket     = []byte("queue")
	metaBucket      = []byte("meta")
)

// BboltStorage implements Storage using bbolt
type BboltStorage struct {
	db *bolt.DB
}

// NewBboltStorage creates a new bbolt-backed storage
func NewBboltStorage(dbPath string) (*BboltStorage, error) {
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open bbolt database: %w", err)
	}

	// Create buckets
	err = db.Update(func(tx *bolt.Tx) error {
		for _, bucket := range [][]byte{jobsBucket, jobStatesBucket, queueBucket, metaBucket} {
			if _, err := tx.CreateBucketIfNotExists(bucket); err != nil {
				return fmt.Errorf("failed to create bucket: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}

	log.Printf("[STORAGE] Bbolt storage initialized at %s", dbPath)
	return &BboltStorage{db: db}, nil
}

// Close closes the database
func (s *BboltStorage) Close() error {
	return s.db.Close()
}

// Jobs

func (s *BboltStorage) SaveJob(job *protocol.Job) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(jobsBucket)
		data, err := encodeJSON(job)
		if err != nil {
			return err
		}
		return b.Put([]byte(job.ID), data)
	})
}

func (s *BboltStorage) LoadJobs() (map[string]*protocol.Job, error) {
	jobs := make(map[string]*protocol.Job)

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(jobsBucket)
		return b.ForEach(func(k, v []byte) error {
			var job protocol.Job
			if err := decodeJSON(v, &job); err != nil {
				log.Printf("[STORAGE] Warning: Failed to decode job %s: %v", k, err)
				return nil // Skip corrupted jobs
			}
			jobs[job.ID] = &job
			return nil
		})
	})

	return jobs, err
}

func (s *BboltStorage) DeleteJob(jobID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(jobsBucket)
		return b.Delete([]byte(jobID))
	})
}

// Job States

func (s *BboltStorage) SaveJobState(jobID string, state *JobState) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(jobStatesBucket)
		data, err := encodeJSON(state)
		if err != nil {
			return err
		}
		return b.Put([]byte(jobID), data)
	})
}

func (s *BboltStorage) LoadJobStates() (map[string]*JobState, error) {
	states := make(map[string]*JobState)

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(jobStatesBucket)
		return b.ForEach(func(k, v []byte) error {
			var state JobState
			if err := decodeJSON(v, &state); err != nil {
				log.Printf("[STORAGE] Warning: Failed to decode job state %s: %v", k, err)
				return nil // Skip corrupted states
			}
			states[string(k)] = &state
			return nil
		})
	})

	return states, err
}

func (s *BboltStorage) DeleteJobState(jobID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(jobStatesBucket)
		return b.Delete([]byte(jobID))
	})
}

// Queue

func (s *BboltStorage) SaveQueue(queue []string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(queueBucket)
		data, err := encodeJSON(queue)
		if err != nil {
			return err
		}
		return b.Put([]byte("queue"), data)
	})
}

func (s *BboltStorage) LoadQueue() ([]string, error) {
	var queue []string

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(queueBucket)
		data := b.Get([]byte("queue"))
		if data == nil {
			return nil // No queue stored
		}
		return decodeJSON(data, &queue)
	})

	return queue, err
}

// Metadata

func (s *BboltStorage) SaveCurrentJobID(jobID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(metaBucket)
		return b.Put([]byte("current_job_id"), []byte(jobID))
	})
}

func (s *BboltStorage) LoadCurrentJobID() (string, error) {
	var jobID string

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(metaBucket)
		data := b.Get([]byte("current_job_id"))
		if data == nil {
			return nil // No current job
		}
		jobID = string(data)
		return nil
	})

	return jobID, err
}
