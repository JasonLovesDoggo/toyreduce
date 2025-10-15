package master

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"strconv"
	"time"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
	"pkg.jsn.cam/toyreduce/pkg/utils/httpx"
)

//go:embed all:static
var uiFS embed.FS

// Server wraps the master and HTTP server
type Server struct {
	master *Master
	mux    *http.ServeMux
}

// NewServer creates a new master server
func NewServer(cfg Config) (*Server, error) {
	master, err := NewMaster(cfg)
	if err != nil {
		return nil, err
	}

	s := &Server{
		master: master,
		mux:    http.NewServeMux(),
	}
	s.setupRoutes()

	// Check store health
	if cfg.StoreURL != "" {
		log.Printf("[MASTER] Checking store health at %s", cfg.StoreURL)
		if err := s.checkStoreHealth(cfg.StoreURL); err != nil {
			return nil, fmt.Errorf("store health check failed: %w", err)
		}
		log.Printf("[MASTER] store is healthy")
	}

	// Start health monitor
	heartbeatTimeout := cfg.HeartbeatTimeout
	if heartbeatTimeout == 0 {
		heartbeatTimeout = 30 * time.Second
	}
	master.StartHealthMonitor(heartbeatTimeout)

	return s, nil
}

func (s *Server) setupRoutes() {
	// Worker APIs
	s.mux.HandleFunc("POST /api/workers/register", httpx.Wrap(s.handleWorkerRegistration))
	s.mux.HandleFunc("GET /api/workers", httpx.Wrap(s.handleWorkerList))
	s.mux.HandleFunc("GET /api/tasks/next", httpx.Wrap(s.handleGetNextTask))
	s.mux.HandleFunc("POST /api/tasks/{taskID}/complete", httpx.Wrap(s.handleTaskCompletion))
	s.mux.HandleFunc("POST /api/workers/{workerID}/heartbeat", httpx.Wrap(s.handleHeartbeat))

	// Job APIs
	s.mux.HandleFunc("POST /api/jobs", httpx.Wrap(s.handleJobSubmit))
	s.mux.HandleFunc("GET /api/jobs", httpx.Wrap(s.handleJobList))
	s.mux.HandleFunc("GET /api/jobs/{jobID}", httpx.Wrap(s.handleJobStatus))
	s.mux.HandleFunc("GET /api/jobs/{jobID}/results", httpx.Wrap(s.handleJobResults))
	s.mux.HandleFunc("POST /api/jobs/{jobID}/cancel", httpx.Wrap(s.handleJobCancel))

	// Config and Status
	s.mux.HandleFunc("GET /api/config", httpx.Wrap(s.handleConfig))
	s.mux.HandleFunc("GET /api/status", httpx.Wrap(s.handleStatus))
	s.mux.HandleFunc("GET /health", httpx.Wrap(s.handleHealth))

	// Store proxy endpoints
	s.mux.HandleFunc("GET /api/store/stats", httpx.Wrap(s.handleStoreStats))
	s.mux.HandleFunc("POST /api/store/reset", httpx.Wrap(s.handleStoreReset))
	s.mux.HandleFunc("POST /api/store/compact", httpx.Wrap(s.handleStoreCompact))
	s.mux.HandleFunc("GET /api/store/health", httpx.Wrap(s.handleStoreHealthCheck))

	// UI - Serve the embedded Svelte app from root
	staticFS, err := fs.Sub(uiFS, "static")
	if err != nil {
		log.Printf("[MASTER] Warning: Failed to load UI: %v", err)
	} else {
		s.mux.Handle("/", http.FileServer(http.FS(staticFS)))
	}
}

func (s *Server) handleWorkerRegistration(w http.ResponseWriter, r *http.Request) error {
	var req protocol.WorkerRegistrationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.Error(w, http.StatusBadRequest, err.Error())
		return nil
	}

	// Validate and register worker
	if err := s.master.RegisterWorker(req.WorkerID, req.Version, req.Executors, req.DataEndpoint); err != nil {
		resp := protocol.WorkerRegistrationResponse{
			WorkerID: req.WorkerID,
			Success:  false,
			Error:    err.Error(),
		}
		httpx.JSON(w, http.StatusOK, resp) // Still 200, but Success=false
		return nil
	}

	resp := protocol.WorkerRegistrationResponse{
		WorkerID: req.WorkerID,
		StoreURL: s.master.storeURL,
		Success:  true,
	}

	httpx.JSON(w, http.StatusOK, resp)
	return nil
}

func (s *Server) handleGetNextTask(w http.ResponseWriter, r *http.Request) error {
	workerID := r.URL.Query().Get("workerID")
	if workerID == "" {
		httpx.Error(w, http.StatusBadRequest, "workerID required")
		return nil
	}

	task := s.master.GetNextTask(workerID)
	httpx.JSON(w, http.StatusOK, task)
	return nil
}

func (s *Server) handleTaskCompletion(w http.ResponseWriter, r *http.Request) error {
	taskID := r.PathValue("taskID")
	workerID := r.URL.Query().Get("workerID")

	if workerID == "" {
		httpx.Error(w, http.StatusBadRequest, "workerID required")
		return nil
	}

	var req protocol.TaskCompletionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.Error(w, http.StatusBadRequest, err.Error())
		return nil
	}

	// Determine task type by checking both task lists
	var acknowledged bool
	var message string

	// Try map task first
	if s.master.CompleteMapTask(taskID, workerID, req.Version, req.Success, req.Error) {
		acknowledged = true
		message = "map task completed"
	} else if s.master.CompleteReduceTask(taskID, workerID, req.Version, req.Success, req.Error) {
		acknowledged = true
		message = "reduce task completed"
	} else {
		message = "task not found or version mismatch"
	}

	resp := protocol.TaskCompletionResponse{
		Acknowledged: acknowledged,
		Message:      message,
	}

	httpx.JSON(w, http.StatusOK, resp)
	return nil
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) error {
	workerID := r.PathValue("workerID")

	var req protocol.HeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.Error(w, http.StatusBadRequest, err.Error())
		return nil
	}

	ok := s.master.UpdateHeartbeat(workerID)

	resp := protocol.HeartbeatResponse{OK: ok}
	httpx.JSON(w, http.StatusOK, resp)
	return nil
}

func (s *Server) handleWorkerList(w http.ResponseWriter, r *http.Request) error {
	workers := s.master.ListWorkers()

	httpx.JSON(w, http.StatusOK, map[string]interface{}{
		"workers": workers,
	})
	return nil
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) error {
	config := s.master.GetConfig()
	httpx.JSON(w, http.StatusOK, config)
	return nil
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) error {
	status := s.master.GetStatus()
	httpx.JSON(w, http.StatusOK, status)
	return nil
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) error {
	httpx.JSON(w, http.StatusOK, protocol.HealthResponse{Status: "ok"})
	return nil
}

// Store proxy handlers
func (s *Server) handleStoreStats(w http.ResponseWriter, r *http.Request) error {
	storeUrl := s.master.storeURL
	if storeUrl == "" {
		httpx.Error(w, http.StatusServiceUnavailable, "store not configured")
		return nil
	}

	resp, err := http.Get(storeUrl + "/stats")
	if err != nil {
		httpx.Error(w, http.StatusServiceUnavailable, fmt.Sprintf("store unreachable: %v", err))
		return nil
	}
	defer resp.Body.Close()

	// Copy response
	var stats map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		httpx.Error(w, http.StatusInternalServerError, err.Error())
		return nil
	}
	httpx.JSON(w, resp.StatusCode, stats)
	return nil
}

func (s *Server) handleStoreReset(w http.ResponseWriter, r *http.Request) error {
	storeURL := s.master.storeURL
	if storeURL == "" {
		httpx.Error(w, http.StatusServiceUnavailable, "store not configured")
		return nil
	}

	req, err := http.NewRequest("POST", storeURL+"/reset", nil)
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, err.Error())
		return nil
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		httpx.Error(w, http.StatusServiceUnavailable, fmt.Sprintf("store unreachable: %v", err))
		return nil
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		httpx.Error(w, http.StatusInternalServerError, err.Error())
		return nil
	}
	httpx.JSON(w, resp.StatusCode, result)
	return nil
}

func (s *Server) handleStoreCompact(w http.ResponseWriter, r *http.Request) error {
	storeUrl := s.master.storeURL
	if storeUrl == "" {
		httpx.Error(w, http.StatusServiceUnavailable, "store not configured")
		return nil
	}

	req, err := http.NewRequest("POST", storeUrl+"/compact", nil)
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, err.Error())
		return nil
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		httpx.Error(w, http.StatusServiceUnavailable, fmt.Sprintf("store unreachable: %v", err))
		return nil
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		httpx.Error(w, http.StatusInternalServerError, err.Error())
		return nil
	}
	httpx.JSON(w, resp.StatusCode, result)
	return nil
}

func (s *Server) handleStoreHealthCheck(w http.ResponseWriter, r *http.Request) error {
	storeUrl := s.master.storeURL
	if storeUrl == "" {
		httpx.JSON(w, http.StatusOK, map[string]interface{}{
			"status":  "unconfigured",
			"healthy": false,
		})
		return nil
	}

	err := s.checkStoreHealth(storeUrl)
	if err != nil {
		httpx.JSON(w, http.StatusOK, map[string]interface{}{
			"status":  "unhealthy",
			"healthy": false,
			"error":   err.Error(),
		})
		return nil
	}

	httpx.JSON(w, http.StatusOK, map[string]interface{}{
		"status":  "healthy",
		"healthy": true,
	})
	return nil
}

func (s *Server) handleJobSubmit(w http.ResponseWriter, r *http.Request) error {
	var req protocol.JobSubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.Error(w, http.StatusBadRequest, err.Error())
		return nil
	}

	jobID, err := s.master.SubmitJob(req)
	if err != nil {
		resp := protocol.JobSubmitResponse{
			Status:  "error",
			Message: err.Error(),
		}
		httpx.JSON(w, http.StatusBadRequest, resp)
		return nil
	}

	resp := protocol.JobSubmitResponse{
		JobID:  jobID,
		Status: "queued",
	}

	httpx.JSON(w, http.StatusCreated, resp)
	return nil
}

func (s *Server) handleJobList(w http.ResponseWriter, r *http.Request) error {
	jobs := s.master.ListJobs()

	// Compute durations for all jobs
	for i := range jobs {
		jobs[i].ComputeDurations()
	}

	resp := protocol.JobListResponse{
		Jobs: jobs,
	}

	httpx.JSON(w, http.StatusOK, resp)
	return nil
}

func (s *Server) handleJobStatus(w http.ResponseWriter, r *http.Request) error {
	jobID := r.PathValue("jobID")

	job := s.master.GetJob(jobID)
	if job == nil {
		httpx.Error(w, http.StatusNotFound, "job not found")
		return nil
	}

	// Compute durations
	job.ComputeDurations()

	httpx.JSON(w, http.StatusOK, job)
	return nil
}

func (s *Server) handleJobResults(w http.ResponseWriter, r *http.Request) error {
	jobID := r.PathValue("jobID")

	results, err := s.master.GetJobResults(jobID)
	if err != nil {
		httpx.Error(w, http.StatusBadRequest, err.Error())
		return nil
	}

	httpx.JSON(w, http.StatusOK, results)
	return nil
}

func (s *Server) handleJobCancel(w http.ResponseWriter, r *http.Request) error {
	jobID := r.PathValue("jobID")

	err := s.master.CancelJob(jobID)
	if err != nil {
		resp := protocol.JobCancelResponse{
			Success: false,
			Message: err.Error(),
		}
		httpx.JSON(w, http.StatusBadRequest, resp)
		return nil
	}

	resp := protocol.JobCancelResponse{
		Success: true,
		Message: "job cancelled",
	}

	httpx.JSON(w, http.StatusOK, resp)
	return nil
}

// Start starts the HTTP server
func (s *Server) Start(port int) error {
	addr := ":" + strconv.Itoa(port)
	log.Printf("[MASTER] Starting master server on %s", addr)
	return http.ListenAndServe(addr, s.mux)
}

// GetMaster returns the underlying master (for testing/CLI)
func (s *Server) GetMaster() *Master {
	return s.master
}

// Close closes the server and cleans up resources
func (s *Server) Close() error {
	if s.master.storage != nil {
		return s.master.storage.Close()
	}
	return nil
}

// checkStoreHealth checks if the store server is responding
func (s *Server) checkStoreHealth(storeURL string) error {
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	resp, err := client.Get(storeURL + "/health")
	if err != nil {
		return fmt.Errorf("store unreachable: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("store returned status %d", resp.StatusCode)
	}

	var health protocol.HealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		return fmt.Errorf("invalid health response: %w", err)
	}

	if health.Status != "ok" {
		return fmt.Errorf("store status is %s", health.Status)
	}

	return nil
}
