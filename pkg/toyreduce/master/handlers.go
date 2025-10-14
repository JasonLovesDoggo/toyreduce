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

	// Check cache health
	if cfg.CacheURL != "" {
		log.Printf("[MASTER] Checking cache health at %s", cfg.CacheURL)
		if err := s.checkCacheHealth(cfg.CacheURL); err != nil {
			return nil, fmt.Errorf("cache health check failed: %w", err)
		}
		log.Printf("[MASTER] Cache is healthy")
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
	s.mux.HandleFunc("POST /api/workers/register", s.handleWorkerRegistration)
	s.mux.HandleFunc("GET /api/workers", s.handleWorkerList)
	s.mux.HandleFunc("GET /api/tasks/next", s.handleGetNextTask)
	s.mux.HandleFunc("POST /api/tasks/{taskID}/complete", s.handleTaskCompletion)
	s.mux.HandleFunc("POST /api/workers/{workerID}/heartbeat", s.handleHeartbeat)

	// Job APIs
	s.mux.HandleFunc("POST /api/jobs", s.handleJobSubmit)
	s.mux.HandleFunc("GET /api/jobs", s.handleJobList)
	s.mux.HandleFunc("GET /api/jobs/{jobID}", s.handleJobStatus)
	s.mux.HandleFunc("GET /api/jobs/{jobID}/results", s.handleJobResults)
	s.mux.HandleFunc("POST /api/jobs/{jobID}/cancel", s.handleJobCancel)

	// Config and Status
	s.mux.HandleFunc("GET /api/config", s.handleConfig)
	s.mux.HandleFunc("GET /api/status", s.handleStatus)
	s.mux.HandleFunc("GET /health", s.handleHealth)

	// Cache proxy endpoints
	s.mux.HandleFunc("GET /api/cache/stats", s.handleCacheStats)
	s.mux.HandleFunc("POST /api/cache/reset", s.handleCacheReset)
	s.mux.HandleFunc("POST /api/cache/compact", s.handleCacheCompact)
	s.mux.HandleFunc("GET /api/cache/health", s.handleCacheHealthCheck)

	// UI - Serve the embedded Svelte app from root
	staticFS, err := fs.Sub(uiFS, "static")
	if err != nil {
		log.Printf("[MASTER] Warning: Failed to load UI: %v", err)
	} else {
		s.mux.Handle("/", http.FileServer(http.FS(staticFS)))
	}
}

func (s *Server) handleWorkerRegistration(w http.ResponseWriter, r *http.Request) {
	var req protocol.WorkerRegistrationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Validate and register worker
	if err := s.master.RegisterWorker(req.WorkerID, req.Version, req.Executors); err != nil {
		resp := protocol.WorkerRegistrationResponse{
			WorkerID: req.WorkerID,
			Success:  false,
			Error:    err.Error(),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK) // Still 200, but Success=false
		json.NewEncoder(w).Encode(resp)
		return
	}

	resp := protocol.WorkerRegistrationResponse{
		WorkerID: req.WorkerID,
		CacheURL: s.master.cacheURL,
		Success:  true,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleGetNextTask(w http.ResponseWriter, r *http.Request) {
	workerID := r.URL.Query().Get("workerID")
	if workerID == "" {
		http.Error(w, "workerID required", http.StatusBadRequest)
		return
	}

	task := s.master.GetNextTask(workerID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(task)
}

func (s *Server) handleTaskCompletion(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("taskID")
	workerID := r.URL.Query().Get("workerID")

	if workerID == "" {
		http.Error(w, "workerID required", http.StatusBadRequest)
		return
	}

	var req protocol.TaskCompletionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
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

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	workerID := r.PathValue("workerID")

	var req protocol.HeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ok := s.master.UpdateHeartbeat(workerID)

	resp := protocol.HeartbeatResponse{OK: ok}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleWorkerList(w http.ResponseWriter, r *http.Request) {
	workers := s.master.ListWorkers()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"workers": workers,
	})
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	config := s.master.GetConfig()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(config)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	status := s.master.GetStatus()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(protocol.HealthResponse{Status: "ok"})
}

// Cache proxy handlers
func (s *Server) handleCacheStats(w http.ResponseWriter, r *http.Request) {
	cacheURL := s.master.cacheURL
	if cacheURL == "" {
		http.Error(w, "cache not configured", http.StatusServiceUnavailable)
		return
	}

	resp, err := http.Get(cacheURL + "/stats")
	if err != nil {
		http.Error(w, fmt.Sprintf("cache unreachable: %v", err), http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)

	// Copy response
	var stats map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(stats)
}

func (s *Server) handleCacheReset(w http.ResponseWriter, r *http.Request) {
	cacheURL := s.master.cacheURL
	if cacheURL == "" {
		http.Error(w, "cache not configured", http.StatusServiceUnavailable)
		return
	}

	req, err := http.NewRequest("POST", cacheURL+"/reset", nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, fmt.Sprintf("cache unreachable: %v", err), http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(result)
}

func (s *Server) handleCacheCompact(w http.ResponseWriter, r *http.Request) {
	cacheURL := s.master.cacheURL
	if cacheURL == "" {
		http.Error(w, "cache not configured", http.StatusServiceUnavailable)
		return
	}

	req, err := http.NewRequest("POST", cacheURL+"/compact", nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, fmt.Sprintf("cache unreachable: %v", err), http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(result)
}

func (s *Server) handleCacheHealthCheck(w http.ResponseWriter, r *http.Request) {
	cacheURL := s.master.cacheURL
	if cacheURL == "" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "unconfigured",
			"healthy": false,
		})
		return
	}

	err := s.checkCacheHealth(cacheURL)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "unhealthy",
			"healthy": false,
			"error":   err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "healthy",
		"healthy": true,
	})
}

func (s *Server) handleJobSubmit(w http.ResponseWriter, r *http.Request) {
	var req protocol.JobSubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	jobID, err := s.master.SubmitJob(req)
	if err != nil {
		resp := protocol.JobSubmitResponse{
			Status:  "error",
			Message: err.Error(),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(resp)
		return
	}

	resp := protocol.JobSubmitResponse{
		JobID:  jobID,
		Status: "queued",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleJobList(w http.ResponseWriter, r *http.Request) {
	jobs := s.master.ListJobs()

	// Compute durations for all jobs
	for i := range jobs {
		jobs[i].ComputeDurations()
	}

	resp := protocol.JobListResponse{
		Jobs: jobs,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleJobStatus(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobID")

	job := s.master.GetJob(jobID)
	if job == nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	// Compute durations
	job.ComputeDurations()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

func (s *Server) handleJobResults(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobID")

	results, err := s.master.GetJobResults(jobID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

func (s *Server) handleJobCancel(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobID")

	err := s.master.CancelJob(jobID)
	if err != nil {
		resp := protocol.JobCancelResponse{
			Success: false,
			Message: err.Error(),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(resp)
		return
	}

	resp := protocol.JobCancelResponse{
		Success: true,
		Message: "job cancelled",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
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

// checkCacheHealth checks if the cache server is responding
func (s *Server) checkCacheHealth(cacheURL string) error {
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	resp, err := client.Get(cacheURL + "/health")
	if err != nil {
		return fmt.Errorf("cache unreachable: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("cache returned status %d", resp.StatusCode)
	}

	var health protocol.HealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		return fmt.Errorf("invalid health response: %w", err)
	}

	if health.Status != "ok" {
		return fmt.Errorf("cache status is %s", health.Status)
	}

	return nil
}
