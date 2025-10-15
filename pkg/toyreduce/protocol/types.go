package protocol

import (
	"time"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce"
)

// TaskStatus represents the state of a task
type TaskStatus string

const (
	TaskStatusIdle       TaskStatus = "idle"
	TaskStatusInProgress TaskStatus = "in_progress"
	TaskStatusCompleted  TaskStatus = "completed"
	TaskStatusFailed     TaskStatus = "failed"
)

// TaskType identifies whether a task is map or reduce
type TaskType string

const (
	TaskTypeMap    TaskType = "map"
	TaskTypeReduce TaskType = "reduce"
	TaskTypeNone   TaskType = "none"
)

// MapTask represents a chunk of data to be mapped
type MapTask struct {
	ID            string     `json:"id"`
	Chunk         []string   `json:"chunk"`
	Status        TaskStatus `json:"status"`
	WorkerID      string     `json:"worker_id"`
	StartTime     time.Time  `json:"start_time"`
	CompletedAt   time.Time  `json:"completed_at,omitempty"`
	Version       string     `json:"version"` // for idempotency
	NumPartitions int        `json:"num_partitions"`
	RetryCount    int        `json:"retry_count"`
}

// ReduceTask represents a partition to be reduced
type ReduceTask struct {
	ID          string     `json:"id"`
	JobID       string     `json:"job_id"`
	Partition   int        `json:"partition"`
	Status      TaskStatus `json:"status"`
	WorkerID    string     `json:"worker_id"`
	StartTime   time.Time  `json:"start_time"`
	CompletedAt time.Time  `json:"completed_at,omitempty"`
	Version     string     `json:"version"` // for idempotency
	RetryCount  int        `json:"retry_count"`
}

// Task is a union type for map and reduce tasks
type Task struct {
	Type       TaskType    `json:"type"`
	MapTask    *MapTask    `json:"map_task,omitempty"`
	ReduceTask *ReduceTask `json:"reduce_task,omitempty"`
}

// WorkerRegistrationRequest is sent by workers to register with master
type WorkerRegistrationRequest struct {
	WorkerID  string   `json:"worker_id"`
	Version   string   `json:"version"`   // ToyReduce version
	Executors []string `json:"executors"` // Supported executors
}

// WorkerRegistrationResponse is returned to workers upon registration
type WorkerRegistrationResponse struct {
	WorkerID string `json:"worker_id"`
	CacheURL string `json:"cache_url"`
	Success  bool   `json:"success"`
	Error    string `json:"error,omitempty"`
}

// TaskCompletionRequest is sent by workers when they complete a task
type TaskCompletionRequest struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
	Version string `json:"version"`
}

// TaskCompletionResponse acknowledges task completion
type TaskCompletionResponse struct {
	Acknowledged bool   `json:"acknowledged"`
	Message      string `json:"message,omitempty"`
}

// HeartbeatRequest is sent periodically by workers to master
type HeartbeatRequest struct {
	Timestamp time.Time `json:"timestamp"`
}

// HeartbeatResponse acknowledges the heartbeat
type HeartbeatResponse struct {
	OK bool `json:"ok"`
}

// StatusResponse provides overall job status
type StatusResponse struct {
	MapTasksTotal      int `json:"map_tasks_total"`
	MapTasksIdle       int `json:"map_tasks_idle"`
	MapTasksInProgress int `json:"map_tasks_in_progress"`
	MapTasksCompleted  int `json:"map_tasks_completed"`
	MapTasksFailed     int `json:"map_tasks_failed"`

	ReduceTasksTotal      int `json:"reduce_tasks_total"`
	ReduceTasksIdle       int `json:"reduce_tasks_idle"`
	ReduceTasksInProgress int `json:"reduce_tasks_in_progress"`
	ReduceTasksCompleted  int `json:"reduce_tasks_completed"`
	ReduceTasksFailed     int `json:"reduce_tasks_failed"`

	WorkersRegistered int    `json:"workers_registered"`
	WorkersActive     int    `json:"workers_active"`
	JobStatus         string `json:"job_status"` // "mapping", "reducing", "completed", "failed"
}

// IntermediateData represents key-value pairs for a partition
type IntermediateData struct {
	TaskID    string               `json:"task_id"`
	JobID     string               `json:"job_id"`
	Partition int                  `json:"partition"`
	Data      []toyreduce.KeyValue `json:"data"`
}

// HealthResponse indicates node health
type HealthResponse struct {
	Status string `json:"status"`
}
