package cache

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
	"pkg.jsn.cam/toyreduce/pkg/utils/httpx"
)

// Server wraps the storage and HTTP server
type Server struct {
	storage *Storage
	mux     *http.ServeMux
}

// NewServer creates a new cache server
func NewServer(dbPath string) (*Server, error) {
	storage, err := NewStorage(dbPath)
	if err != nil {
		return nil, err
	}

	s := &Server{
		storage: storage,
		mux:     http.NewServeMux(),
	}
	s.setupRoutes()
	return s, nil
}

// Close closes the server and underlying storage
func (s *Server) Close() error {
	return s.storage.Close()
}

func (s *Server) setupRoutes() {
	// Intermediate map outputs
	s.mux.HandleFunc("POST /intermediate/map/{taskID}/{partition}", httpx.Wrap(s.handleStoreMapOutput))
	s.mux.HandleFunc("GET /intermediate/reduce/{partition}", httpx.Wrap(s.handleGetReduceInput))

	// Final reduce outputs
	s.mux.HandleFunc("POST /results/{taskID}", httpx.Wrap(s.handleStoreReduceOutput))
	s.mux.HandleFunc("GET /results/job/{jobID}", httpx.Wrap(s.handleGetJobResults))
	s.mux.HandleFunc("GET /results/{taskID}", httpx.Wrap(s.handleGetReduceOutput))

	// Management
	s.mux.HandleFunc("POST /reset", httpx.Wrap(s.handleReset))
	s.mux.HandleFunc("POST /compact", httpx.Wrap(s.handleCompact))
	s.mux.HandleFunc("GET /health", httpx.Wrap(s.handleHealth))
	s.mux.HandleFunc("GET /stats", httpx.Wrap(s.handleStats))
}

func (s *Server) handleStoreMapOutput(w http.ResponseWriter, r *http.Request) error {
	partitionStr := r.PathValue("partition")
	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid partition")
		return nil
	}

	var data protocol.IntermediateData
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		httpx.Error(w, http.StatusBadRequest, err.Error())
		return nil
	}

	s.storage.StoreMapOutput(partition, data.Data)
	log.Printf("[CACHE] Stored %d KVs for partition %d from task %s", len(data.Data), partition, data.TaskID)

	httpx.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
	return nil
}

func (s *Server) handleGetReduceInput(w http.ResponseWriter, r *http.Request) error {
	partitionStr := r.PathValue("partition")
	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid partition")
		return nil
	}

	data := s.storage.GetReduceInput(partition)
	log.Printf("[CACHE] Retrieved %d KVs for partition %d", len(data), partition)

	httpx.JSON(w, http.StatusOK, data)
	return nil
}

func (s *Server) handleStoreReduceOutput(w http.ResponseWriter, r *http.Request) error {
	taskID := r.PathValue("taskID")

	var data protocol.IntermediateData
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		httpx.Error(w, http.StatusBadRequest, err.Error())
		return nil
	}

	if data.JobID == "" {
		httpx.Error(w, http.StatusBadRequest, "job_id is required")
		return nil
	}

	s.storage.StoreReduceOutput(taskID, data.JobID, data.Data)
	log.Printf("[CACHE] Stored reduce results for job %s task %s (%d KVs)", data.JobID, taskID, len(data.Data))

	httpx.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
	return nil
}

func (s *Server) handleGetReduceOutput(w http.ResponseWriter, r *http.Request) error {
	taskID := r.PathValue("taskID")

	data, exists := s.storage.GetReduceOutput(taskID)
	if !exists {
		httpx.Error(w, http.StatusNotFound, "task not found")
		return nil
	}

	httpx.JSON(w, http.StatusOK, data)
	return nil
}

func (s *Server) handleGetJobResults(w http.ResponseWriter, r *http.Request) error {
	jobID := r.PathValue("jobID")

	data := s.storage.GetJobResults(jobID)
	log.Printf("[CACHE] Retrieved results for job %s (%d KVs)", jobID, len(data))

	httpx.JSON(w, http.StatusOK, data)
	return nil
}

func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) error {
	if err := s.storage.Reset(); err != nil {
		log.Printf("[CACHE] Reset error: %v", err)
		httpx.Error(w, http.StatusInternalServerError, err.Error())
		return nil
	}
	log.Println("[CACHE] Storage reset")

	httpx.JSON(w, http.StatusOK, map[string]string{"status": "reset"})
	return nil
}

func (s *Server) handleCompact(w http.ResponseWriter, r *http.Request) error {
	log.Println("[CACHE] Starting database compaction")

	if err := s.storage.Compact(); err != nil {
		log.Printf("[CACHE] Compact error: %v", err)
		httpx.Error(w, http.StatusInternalServerError, err.Error())
		return nil
	}

	log.Println("[CACHE] Database compacted successfully")

	httpx.JSON(w, http.StatusOK, map[string]string{"status": "compacted"})
	return nil
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) error {
	httpx.JSON(w, http.StatusOK, protocol.HealthResponse{Status: "ok"})
	return nil
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) error {
	stats := s.storage.Stats()
	httpx.JSON(w, http.StatusOK, stats)
	return nil
}

// Start starts the HTTP server
func (s *Server) Start(port int) error {
	addr := ":" + strconv.Itoa(port)
	log.Printf("[CACHE] Starting cache server on %s", addr)
	return http.ListenAndServe(addr, s.mux)
}
