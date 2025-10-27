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
	StartTime     time.Time  `json:"start_time"`
	CompletedAt   time.Time  `json:"completed_at,omitempty"`
	ID            string     `json:"id"`
	JobID         string     `json:"job_id"`
	Executor      string     `json:"executor"` // executor name for this task
	Status        TaskStatus `json:"status"`
	WorkerID      string     `json:"worker_id"`
	Version       string     `json:"version"` // for idempotency
	Chunk         []string   `json:"chunk"`
	NumPartitions int        `json:"num_partitions"`
	RetryCount    int        `json:"retry_count"`
}

// ReduceTask represents a partition to be reduced
type ReduceTask struct {
	StartTime       time.Time  `json:"start_time"`
	CompletedAt     time.Time  `json:"completed_at,omitempty"`
	ID              string     `json:"id"`
	JobID           string     `json:"job_id"`
	Executor        string     `json:"executor"` // executor name for this task
	Status          TaskStatus `json:"status"`
	WorkerID        string     `json:"worker_id"`
	Version         string     `json:"version"` // for idempotency
	WorkerEndpoints []string   `json:"worker_endpoints,omitempty"`
	Partition       int        `json:"partition"`
	RetryCount      int        `json:"retry_count"`
}

// Task is a union type for map and reduce tasks
type Task struct {
	MapTask    *MapTask    `json:"map_task,omitempty"`
	ReduceTask *ReduceTask `json:"reduce_task,omitempty"`
	Type       TaskType    `json:"type"`
}

// WorkerRegistrationRequest is sent by workers to register with master
type WorkerRegistrationRequest struct {
	WorkerID     string   `json:"worker_id"`
	Version      string   `json:"version"`       // ToyReduce version
	DataEndpoint string   `json:"data_endpoint"` // HTTP endpoint for serving partition data
	Executors    []string `json:"executors"`     // Supported executors
}

// WorkerRegistrationResponse is returned to workers upon registration
type WorkerRegistrationResponse struct {
	WorkerID string `json:"worker_id"`
	StoreURL string `json:"store_url"`
	Error    string `json:"error,omitempty"`
	Success  bool   `json:"success"`
}

// TaskCompletionRequest is sent by workers when they complete a task
type TaskCompletionRequest struct {
	Error   string `json:"error,omitempty"`
	Version string `json:"version"`
	Success bool   `json:"success"`
}

// TaskCompletionResponse acknowledges task completion
type TaskCompletionResponse struct {
	Message      string `json:"message,omitempty"`
	Acknowledged bool   `json:"acknowledged"`
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
	JobStatus             string `json:"job_status"` // "mapping", "reducing", "completed", "failed"
	ReduceTasksIdle       int    `json:"reduce_tasks_idle"`
	MapTasksInProgress    int    `json:"map_tasks_in_progress"`
	MapTasksCompleted     int    `json:"map_tasks_completed"`
	MapTasksFailed        int    `json:"map_tasks_failed"`
	ReduceTasksTotal      int    `json:"reduce_tasks_total"`
	MapTasksTotal         int    `json:"map_tasks_total"`
	ReduceTasksInProgress int    `json:"reduce_tasks_in_progress"`
	ReduceTasksCompleted  int    `json:"reduce_tasks_completed"`
	ReduceTasksFailed     int    `json:"reduce_tasks_failed"`
	WorkersRegistered     int    `json:"workers_registered"`
	WorkersActive         int    `json:"workers_active"`
	MapTasksIdle          int    `json:"map_tasks_idle"`
}

// IntermediateData represents key-value pairs for a partition
type IntermediateData struct {
	TaskID    string               `json:"task_id"`
	JobID     string               `json:"job_id"`
	Data      []toyreduce.KeyValue `json:"data"`
	Partition int                  `json:"partition"`
}

// HealthResponse indicates node health
type HealthResponse struct {
	Status string `json:"status"`
}
