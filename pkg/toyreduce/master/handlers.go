package master

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
)

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

	// Start health monitor
	heartbeatTimeout := cfg.HeartbeatTimeout
	if heartbeatTimeout == 0 {
		heartbeatTimeout = 30 * time.Second
	}
	master.StartHealthMonitor(heartbeatTimeout)

	return s, nil
}

func (s *Server) setupRoutes() {
	s.mux.HandleFunc("POST /api/workers/register", s.handleWorkerRegistration)
	s.mux.HandleFunc("GET /api/tasks/next", s.handleGetNextTask)
	s.mux.HandleFunc("POST /api/tasks/{taskID}/complete", s.handleTaskCompletion)
	s.mux.HandleFunc("POST /api/workers/{workerID}/heartbeat", s.handleHeartbeat)
	s.mux.HandleFunc("GET /api/status", s.handleStatus)
	s.mux.HandleFunc("GET /health", s.handleHealth)
}

func (s *Server) handleWorkerRegistration(w http.ResponseWriter, r *http.Request) {
	var req protocol.WorkerRegistrationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.master.RegisterWorker(req.WorkerID)

	resp := protocol.WorkerRegistrationResponse{
		WorkerID:     req.WorkerID,
		CacheURL:     s.master.cacheURL,
		ExecutorName: s.master.executorName,
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

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	status := s.master.GetStatus()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(protocol.HealthResponse{Status: "ok"})
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
