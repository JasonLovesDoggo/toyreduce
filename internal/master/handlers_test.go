package master

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
)

// createTestServer creates a test server with a test master
func createTestServer() *Server {
	master := createTestMaster()
	s := &Server{
		master: master,
		mux:    http.NewServeMux(),
	}
	s.setupRoutes()

	return s
}

func TestHandleJobSubmit(t *testing.T) {
	server := createTestServer()

	submitReq := protocol.JobSubmitRequest{
		Executor:    "wordcount",
		InputPath:   "/tmp/input.txt",
		ChunkSize:   16,
		ReduceTasks: 4,
	}

	body, _ := json.Marshal(submitReq)
	req := httptest.NewRequest(http.MethodPost, "/api/jobs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()

	server.mux.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("Expected status 201, got %d. Body: %s", w.Code, w.Body.String())
	}

	var resp protocol.JobSubmitResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp.JobID == "" {
		t.Error("Expected JobID in response")
	}

	if resp.Status != "queued" {
		t.Errorf("Expected status 'queued', got %s", resp.Status)
	}
}

func TestHandleJobSubmit_InvalidExecutor(t *testing.T) {
	server := createTestServer()

	submitReq := protocol.JobSubmitRequest{
		Executor:    "nonexistent",
		InputPath:   "/tmp/input.txt",
		ChunkSize:   16,
		ReduceTasks: 4,
	}

	body, _ := json.Marshal(submitReq)
	req := httptest.NewRequest(http.MethodPost, "/api/jobs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()

	server.mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400 for invalid executor, got %d", w.Code)
	}
}

func TestHandleJobStatus(t *testing.T) {
	server := createTestServer()

	// Create a job first
	jobID := "test-job-1"
	server.master.jobs[jobID] = &protocol.Job{
		ID:       jobID,
		Status:   protocol.JobStatusQueued,
		Executor: "wordcount",
	}

	req := httptest.NewRequest(http.MethodGet, "/api/jobs/"+jobID, nil)
	w := httptest.NewRecorder()

	server.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d. Body: %s", w.Code, w.Body.String())
	}

	var job protocol.Job
	if err := json.NewDecoder(w.Body).Decode(&job); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if job.ID != jobID {
		t.Errorf("Expected job ID %s, got %s", jobID, job.ID)
	}

	if job.Status != protocol.JobStatusQueued {
		t.Errorf("Expected status queued, got %s", job.Status)
	}
}

func TestHandleJobStatus_NotFound(t *testing.T) {
	server := createTestServer()

	req := httptest.NewRequest(http.MethodGet, "/api/jobs/nonexistent", nil)
	w := httptest.NewRecorder()

	server.mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status 404 for nonexistent job, got %d", w.Code)
	}
}

func TestHandleWorkerRegistration(t *testing.T) {
	server := createTestServer()

	regReq := protocol.WorkerRegistrationRequest{
		WorkerID:     "worker-1",
		Version:      "v0.2.0",
		Executors:    []string{"wordcount", "actioncount"},
		DataEndpoint: "http://localhost:9000",
	}

	body, _ := json.Marshal(regReq)
	req := httptest.NewRequest(http.MethodPost, "/api/workers/register", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()

	server.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d. Body: %s", w.Code, w.Body.String())
	}

	var resp protocol.WorkerRegistrationResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if !resp.Success {
		t.Errorf("Expected success=true, got false. Error: %s", resp.Error)
	}

	if resp.WorkerID != "worker-1" {
		t.Errorf("Expected worker ID 'worker-1', got %s", resp.WorkerID)
	}

	// Verify worker was registered
	server.master.mu.RLock()
	worker, exists := server.master.workers["worker-1"]
	server.master.mu.RUnlock()

	if !exists {
		t.Fatal("Worker not found in master registry")
	}

	if worker.DataEndpoint != "http://localhost:9000" {
		t.Errorf("Expected endpoint http://localhost:9000, got %s", worker.DataEndpoint)
	}
}

func TestHandleGetNextTask_NoJob(t *testing.T) {
	server := createTestServer()

	req := httptest.NewRequest(http.MethodGet, "/api/tasks/next?workerID=worker-1", nil)
	w := httptest.NewRecorder()

	server.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}

	var task protocol.Task
	if err := json.NewDecoder(w.Body).Decode(&task); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if task.Type != protocol.TaskTypeNone {
		t.Errorf("Expected TaskTypeNone when no jobs, got %s", task.Type)
	}
}

func TestHandleGetNextTask_WithMapTask(t *testing.T) {
	server := createTestServer()

	// Setup a job with map tasks
	jobID := "job-1"
	server.master.currentJobID = jobID
	server.master.jobs[jobID] = &protocol.Job{
		ID:     jobID,
		Status: protocol.JobStatusRunning,
	}
	server.master.jobStates[jobID] = &JobState{
		mapTasks: []*protocol.MapTask{
			{
				ID:       "map-1",
				Executor: "wordcount",
				Status:   protocol.TaskStatusIdle,
				Chunk:    []string{"line1", "line2"},
			},
		},
		mapTasksLeft:       1,
		mapWorkerEndpoints: make(map[string]string),
	}
	server.master.workers["worker-1"] = &WorkerInfo{
		ID:           "worker-1",
		DataEndpoint: "http://localhost:9000",
	}

	req := httptest.NewRequest(http.MethodGet, "/api/tasks/next?workerID=worker-1", nil)
	w := httptest.NewRecorder()

	server.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d. Body: %s", w.Code, w.Body.String())
	}

	var task protocol.Task
	if err := json.NewDecoder(w.Body).Decode(&task); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if task.Type != protocol.TaskTypeMap {
		t.Errorf("Expected TaskTypeMap, got %s", task.Type)
	}

	if task.MapTask == nil {
		t.Fatal("MapTask is nil")
	}

	if task.MapTask.ID != "map-1" {
		t.Errorf("Expected task ID 'map-1', got %s", task.MapTask.ID)
	}
}

func TestHandleJobList(t *testing.T) {
	server := createTestServer()

	// Add some jobs
	server.master.jobs["job-1"] = &protocol.Job{
		ID:       "job-1",
		Status:   protocol.JobStatusQueued,
		Executor: "wordcount",
	}
	server.master.jobs["job-2"] = &protocol.Job{
		ID:       "job-2",
		Status:   protocol.JobStatusRunning,
		Executor: "actioncount",
	}

	req := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
	w := httptest.NewRecorder()

	server.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}

	var resp protocol.JobListResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(resp.Jobs) != 2 {
		t.Errorf("Expected 2 jobs, got %d", len(resp.Jobs))
	}
}

func TestHandleWorkerList(t *testing.T) {
	server := createTestServer()

	// Register some workers
	server.master.workers["worker-1"] = &WorkerInfo{
		ID:        "worker-1",
		Version:   "v0.2.0",
		Executors: []string{"wordcount"},
	}
	server.master.workers["worker-2"] = &WorkerInfo{
		ID:        "worker-2",
		Version:   "v0.2.0",
		Executors: []string{"actioncount"},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/workers", nil)
	w := httptest.NewRecorder()

	server.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}

	var resp map[string][]WorkerInfo
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	workers, ok := resp["workers"]
	if !ok {
		t.Fatal("Response missing 'workers' key")
	}

	if len(workers) != 2 {
		t.Errorf("Expected 2 workers, got %d", len(workers))
	}
}

func TestHandleHealth(t *testing.T) {
	server := createTestServer()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	server.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if status, ok := resp["status"].(string); !ok || status != "ok" {
		t.Errorf("Expected status 'ok', got %v", resp["status"])
	}
}
