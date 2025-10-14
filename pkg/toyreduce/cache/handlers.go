package cache

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
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
	s.mux.HandleFunc("POST /intermediate/map/{taskID}/{partition}", s.handleStoreMapOutput)
	s.mux.HandleFunc("GET /intermediate/reduce/{partition}", s.handleGetReduceInput)

	// Final reduce outputs
	s.mux.HandleFunc("POST /results/{taskID}", s.handleStoreReduceOutput)
	s.mux.HandleFunc("GET /results/{taskID}", s.handleGetReduceOutput)
	s.mux.HandleFunc("GET /results", s.handleGetAllResults)

	// Management
	s.mux.HandleFunc("POST /reset", s.handleReset)
	s.mux.HandleFunc("POST /compact", s.handleCompact)
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("GET /stats", s.handleStats)
}

func (s *Server) handleStoreMapOutput(w http.ResponseWriter, r *http.Request) {
	partitionStr := r.PathValue("partition")
	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		http.Error(w, "invalid partition", http.StatusBadRequest)
		return
	}

	var data protocol.IntermediateData
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.storage.StoreMapOutput(partition, data.Data)
	log.Printf("[CACHE] Stored %d KVs for partition %d from task %s", len(data.Data), partition, data.TaskID)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleGetReduceInput(w http.ResponseWriter, r *http.Request) {
	partitionStr := r.PathValue("partition")
	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		http.Error(w, "invalid partition", http.StatusBadRequest)
		return
	}

	data := s.storage.GetReduceInput(partition)
	log.Printf("[CACHE] Retrieved %d KVs for partition %d", len(data), partition)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func (s *Server) handleStoreReduceOutput(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("taskID")

	var data protocol.IntermediateData
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.storage.StoreReduceOutput(taskID, data.Data)
	log.Printf("[CACHE] Stored reduce results for task %s (%d KVs)", taskID, len(data.Data))

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleGetReduceOutput(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("taskID")

	data, exists := s.storage.GetReduceOutput(taskID)
	if !exists {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func (s *Server) handleGetAllResults(w http.ResponseWriter, r *http.Request) {
	data := s.storage.GetAllReduceOutputs()
	log.Printf("[CACHE] Retrieved all results (%d KVs)", len(data))

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	if err := s.storage.Reset(); err != nil {
		log.Printf("[CACHE] Reset error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Println("[CACHE] Storage reset")

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "reset"})
}

func (s *Server) handleCompact(w http.ResponseWriter, r *http.Request) {
	log.Println("[CACHE] Starting database compaction")

	if err := s.storage.Compact(); err != nil {
		log.Printf("[CACHE] Compact error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Println("[CACHE] Database compacted successfully")

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "compacted"})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(protocol.HealthResponse{Status: "ok"})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := s.storage.Stats()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// Start starts the HTTP server
func (s *Server) Start(port int) error {
	addr := ":" + strconv.Itoa(port)
	log.Printf("[CACHE] Starting cache server on %s", addr)
	return http.ListenAndServe(addr, s.mux)
}
