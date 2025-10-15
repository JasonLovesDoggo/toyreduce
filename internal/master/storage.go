package master

import (
	"encoding/json"
	"fmt"
	"log"

	"pkg.jsn.cam/toyreduce/pkg/storage"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
)

var (
	jobsBucket      = []byte("jobs")
	jobStatesBucket = []byte("job_states")
	queueBucket     = []byte("queue")
	metaBucket      = []byte("meta")
)

// Storage defines the interface for persisting master state
type Storage interface {
	// Jobs
	SaveJob(job *protocol.Job) error
	LoadJobs() (map[string]*protocol.Job, error)
	DeleteJob(jobID string) error

	// Job States
	SaveJobState(jobID string, state *JobState) error
	LoadJobStates() (map[string]*JobState, error)
	DeleteJobState(jobID string) error

	// Queue
	SaveQueue(queue []string) error
	LoadQueue() ([]string, error)

	// Metadata
	SaveCurrentJobID(jobID string) error
	LoadCurrentJobID() (string, error)

	// Cleanup
	Close() error
}

// SerializableJobState is a JSON-serializable version of JobState
type SerializableJobState struct {
	MapTasks        []*protocol.MapTask    `json:"map_tasks"`
	ReduceTasks     []*protocol.ReduceTask `json:"reduce_tasks"`
	MapTasksLeft    int                    `json:"map_tasks_left"`
	ReduceTasksLeft int                    `json:"reduce_tasks_left"`
	ExecutorName    string                 `json:"executor_name"` // Store executor name to recreate worker
}

// ToSerializable converts JobState to SerializableJobState
func (js *JobState) ToSerializable() *SerializableJobState {
	// Get executor name from worker
	executorName := ""
	if js.worker != nil {
		executorName = js.worker.Description() // We'll need to map this back
	}

	return &SerializableJobState{
		MapTasks:        js.mapTasks,
		ReduceTasks:     js.reduceTasks,
		MapTasksLeft:    js.mapTasksLeft,
		ReduceTasksLeft: js.reduceTasksLeft,
		ExecutorName:    executorName,
	}
}

// MarshalJSON implements json.Marshaler for JobState
func (js *JobState) MarshalJSON() ([]byte, error) {
	return json.Marshal(js.ToSerializable())
}

// UnmarshalJSON implements json.Unmarshaler for JobState
func (js *JobState) UnmarshalJSON(data []byte) error {
	var s SerializableJobState
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	js.mapTasks = s.MapTasks
	js.reduceTasks = s.ReduceTasks
	js.mapTasksLeft = s.MapTasksLeft
	js.reduceTasksLeft = s.ReduceTasksLeft
	// worker will be set during restore

	return nil
}

// MasterStorage implements Storage using a storage.Backend
type MasterStorage struct {
	backend storage.Backend
}

// NewMasterStorage creates a new master storage with the given backend
func NewMasterStorage(backend storage.Backend) (*MasterStorage, error) {
	s := &MasterStorage{backend: backend}

	// Initialize buckets
	for _, bucket := range [][]byte{jobsBucket, jobStatesBucket, queueBucket, metaBucket} {
		if err := s.backend.CreateBucket(bucket); err != nil {
			return nil, fmt.Errorf("failed to create bucket: %w", err)
		}
	}

	return s, nil
}

// SaveJob persists a job
func (s *MasterStorage) SaveJob(job *protocol.Job) error {
	data, err := storage.EncodeJSON(job)
	if err != nil {
		return err
	}
	return storage.PutString(s.backend, jobsBucket, job.ID, data)
}

// LoadJobs loads all jobs
func (s *MasterStorage) LoadJobs() (map[string]*protocol.Job, error) {
	jobs := make(map[string]*protocol.Job)

	err := s.backend.ForEach(jobsBucket, func(k, v []byte) error {
		var job protocol.Job
		if err := storage.DecodeJSON(v, &job); err != nil {
			log.Printf("[STORAGE] Warning: Failed to decode job %s: %v", k, err)
			return nil // Skip corrupted jobs
		}
		jobs[job.ID] = &job
		return nil
	})

	return jobs, err
}

// DeleteJob deletes a job
func (s *MasterStorage) DeleteJob(jobID string) error {
	return storage.DeleteString(s.backend, jobsBucket, jobID)
}

// SaveJobState persists a job state
func (s *MasterStorage) SaveJobState(jobID string, state *JobState) error {
	data, err := storage.EncodeJSON(state)
	if err != nil {
		return err
	}
	return storage.PutString(s.backend, jobStatesBucket, jobID, data)
}

// LoadJobStates loads all job states
func (s *MasterStorage) LoadJobStates() (map[string]*JobState, error) {
	states := make(map[string]*JobState)

	err := s.backend.ForEach(jobStatesBucket, func(k, v []byte) error {
		var state JobState
		if err := storage.DecodeJSON(v, &state); err != nil {
			log.Printf("[STORAGE] Warning: Failed to decode job state %s: %v", k, err)
			return nil // Skip corrupted states
		}
		states[string(k)] = &state
		return nil
	})

	return states, err
}

// DeleteJobState deletes a job state
func (s *MasterStorage) DeleteJobState(jobID string) error {
	return storage.DeleteString(s.backend, jobStatesBucket, jobID)
}

// SaveQueue persists the job queue
func (s *MasterStorage) SaveQueue(queue []string) error {
	data, err := storage.EncodeJSON(queue)
	if err != nil {
		return err
	}
	return storage.PutString(s.backend, queueBucket, "queue", data)
}

// LoadQueue loads the job queue
func (s *MasterStorage) LoadQueue() ([]string, error) {
	var queue []string

	data, err := storage.GetString(s.backend, queueBucket, "queue")
	if err != nil {
		return nil, err
	}
	if data == nil {
		return []string{}, nil // No queue stored
	}

	if err := storage.DecodeJSON(data, &queue); err != nil {
		return nil, err
	}

	return queue, nil
}

// SaveCurrentJobID persists the current job ID
func (s *MasterStorage) SaveCurrentJobID(jobID string) error {
	return storage.PutString(s.backend, metaBucket, "current_job_id", []byte(jobID))
}

// LoadCurrentJobID loads the current job ID
func (s *MasterStorage) LoadCurrentJobID() (string, error) {
	data, err := storage.GetString(s.backend, metaBucket, "current_job_id")
	if err != nil {
		return "", err
	}
	if data == nil {
		return "", nil // No current job
	}
	return string(data), nil
}

// Close closes the storage backend
func (s *MasterStorage) Close() error {
	return s.backend.Close()
}

// NewNoOpStorage creates a no-op storage (uses memory backend)
func NewNoOpStorage() Storage {
	backend := storage.NewMemoryBackend()
	ms, _ := NewMasterStorage(backend)
	return ms
}
