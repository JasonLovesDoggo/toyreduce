package protocol

import (
	"time"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce"
)

// JobStatus represents the current state of a job
type JobStatus string

const (
	JobStatusQueued    JobStatus = "queued"
	JobStatusRunning   JobStatus = "running"
	JobStatusCompleted JobStatus = "completed"
	JobStatusFailed    JobStatus = "failed"
	JobStatusCancelled JobStatus = "cancelled"
)

// Job represents a MapReduce job
type Job struct {
	ID          string    `json:"id"`
	Status      JobStatus `json:"status"`
	Executor    string    `json:"executor"`
	InputPath   string    `json:"input_path"`
	OutputPath  string    `json:"output_path,omitempty"`
	ChunkSize   int       `json:"chunk_size"`
	ReduceTasks int       `json:"reduce_tasks"`

	// Progress
	TotalTasks       int `json:"total_tasks"`
	CompletedTasks   int `json:"completed_tasks"`
	MapTasksTotal    int `json:"map_tasks_total"`
	MapTasksDone     int `json:"map_tasks_done"`
	ReduceTasksTotal int `json:"reduce_tasks_total"`
	ReduceTasksDone  int `json:"reduce_tasks_done"`

	// Results
	Results []toyreduce.KeyValue `json:"results,omitempty"`

	// Metadata
	SubmittedAt         time.Time `json:"submitted_at"`
	StartedAt           time.Time `json:"started_at,omitempty"`
	MapPhaseCompletedAt time.Time `json:"map_phase_completed_at,omitempty"`
	CompletedAt         time.Time `json:"completed_at,omitempty"`
	Error               string    `json:"error,omitempty"`

	// Computed durations (in seconds)
	Duration            float64 `json:"duration,omitempty"`              // Total job duration
	MapPhaseDuration    float64 `json:"map_phase_duration,omitempty"`    // Time for map phase
	ReducePhaseDuration float64 `json:"reduce_phase_duration,omitempty"` // Time for reduce phase
}

// ComputeDurations calculates and populates duration fields
func (j *Job) ComputeDurations() {
	// Only compute if job has started
	if j.StartedAt.IsZero() {
		return
	}

	// Total duration
	if !j.CompletedAt.IsZero() {
		j.Duration = j.CompletedAt.Sub(j.StartedAt).Seconds()
	} else if j.Status == JobStatusRunning {
		// For running jobs, calculate elapsed time
		j.Duration = time.Since(j.StartedAt).Seconds()
	}

	// Map phase duration: from start until map phase completion
	if !j.MapPhaseCompletedAt.IsZero() {
		j.MapPhaseDuration = j.MapPhaseCompletedAt.Sub(j.StartedAt).Seconds()
	} else if j.Status == JobStatusRunning && j.MapTasksDone < j.MapTasksTotal {
		// Map phase still in progress
		j.MapPhaseDuration = time.Since(j.StartedAt).Seconds()
	}

	// Reduce phase duration: from map completion to job completion
	if !j.MapPhaseCompletedAt.IsZero() {
		if !j.CompletedAt.IsZero() {
			j.ReducePhaseDuration = j.CompletedAt.Sub(j.MapPhaseCompletedAt).Seconds()
		} else if j.Status == JobStatusRunning {
			// Reduce phase in progress
			j.ReducePhaseDuration = time.Since(j.MapPhaseCompletedAt).Seconds()
		}
	}
}

// JobSubmitRequest is used to submit a new job
type JobSubmitRequest struct {
	Executor    string `json:"executor"`
	InputPath   string `json:"input_path"`
	OutputPath  string `json:"output_path,omitempty"`
	ChunkSize   int    `json:"chunk_size"`
	ReduceTasks int    `json:"reduce_tasks"`
}

// JobSubmitResponse is returned after submitting a job
type JobSubmitResponse struct {
	JobID   string `json:"job_id"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// JobListResponse returns a list of jobs
type JobListResponse struct {
	Jobs []Job `json:"jobs"`
}

// JobCancelRequest is used to cancel a job
type JobCancelRequest struct {
	JobID string `json:"job_id"`
}

// JobCancelResponse is returned after cancelling a job
type JobCancelResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

// UIJobsResponse aggregates data for the UI
type UIJobsResponse struct {
	Workers []UIWorker `json:"workers"`
	Jobs    []Job      `json:"jobs"`
}

// UIWorker represents worker info for the UI
type UIWorker struct {
	ID            string    `json:"id"`
	Executors     []string  `json:"executors"`
	CurrentTask   string    `json:"current_task,omitempty"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	Online        bool      `json:"online"`
}
