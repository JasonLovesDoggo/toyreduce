package master

import (
	"encoding/json"
	"fmt"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
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

// NoOpStorage is a storage implementation that doesn't persist anything
type NoOpStorage struct{}

func NewNoOpStorage() *NoOpStorage {
	return &NoOpStorage{}
}

func (s *NoOpStorage) SaveJob(job *protocol.Job) error { return nil }
func (s *NoOpStorage) LoadJobs() (map[string]*protocol.Job, error) {
	return make(map[string]*protocol.Job), nil
}
func (s *NoOpStorage) DeleteJob(jobID string) error                     { return nil }
func (s *NoOpStorage) SaveJobState(jobID string, state *JobState) error { return nil }
func (s *NoOpStorage) LoadJobStates() (map[string]*JobState, error) {
	return make(map[string]*JobState), nil
}
func (s *NoOpStorage) DeleteJobState(jobID string) error   { return nil }
func (s *NoOpStorage) SaveQueue(queue []string) error      { return nil }
func (s *NoOpStorage) LoadQueue() ([]string, error)        { return []string{}, nil }
func (s *NoOpStorage) SaveCurrentJobID(jobID string) error { return nil }
func (s *NoOpStorage) LoadCurrentJobID() (string, error)   { return "", nil }
func (s *NoOpStorage) Close() error                        { return nil }

// Helper functions for storage implementations
func encodeJSON(v interface{}) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("failed to encode JSON: %w", err)
	}
	return data, nil
}

func decodeJSON(data []byte, v interface{}) error {
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("failed to decode JSON: %w", err)
	}
	return nil
}
