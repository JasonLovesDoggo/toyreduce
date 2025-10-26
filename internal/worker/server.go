package worker

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"strconv"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
)

// Server serves partition data to other workers
type Server struct {
	storage  *Storage
	mux      *http.ServeMux
	listener net.Listener
	endpoint string // Full URL (e.g., "http://192.168.1.5:9001")
}

// NewServer creates a new worker data server
func NewServer(storage *Storage) (*Server, error) {
	// Listen on ephemeral port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, err
	}

	// Get the actual port
	port := listener.Addr().(*net.TCPAddr).Port

	// Get local IP
	ip, err := getLocalIP()
	if err != nil {
		listener.Close()
		return nil, err
	}

	endpoint := "http://" + ip + ":" + strconv.Itoa(port)

	s := &Server{
		storage:  storage,
		mux:      http.NewServeMux(),
		listener: listener,
		endpoint: endpoint,
	}

	s.setupRoutes()

	return s, nil
}

func (s *Server) setupRoutes() {
	s.mux.HandleFunc("GET /data/{jobID}/partition/{partition}", s.handleGetPartition)
	s.mux.HandleFunc("POST /cleanup/{jobID}", s.handleCleanup)
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("GET /stats", s.handleStats)
}

func (s *Server) handleGetPartition(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobID")
	partitionStr := r.PathValue("partition")

	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		http.Error(w, "invalid partition", http.StatusBadRequest)
		return
	}

	data, err := s.storage.GetPartition(jobID, partition)
	if err != nil {
		log.Printf("[WORKER-SERVER] Error getting partition %d for job %s: %v", partition, jobID, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	log.Printf("[WORKER-SERVER] Serving partition %d for job %s (%d KVs)", partition, jobID, len(data))

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func (s *Server) handleCleanup(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobID")

	if err := s.storage.CleanupJob(jobID); err != nil {
		log.Printf("[WORKER-SERVER] Error cleaning up job %s: %v", jobID, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	log.Printf("[WORKER-SERVER] Cleaned up job %s", jobID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
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

// Start starts the HTTP server (non-blocking)
func (s *Server) Start() {
	log.Printf("[WORKER-SERVER] Starting data server on %s", s.endpoint)

	go func() {
		if err := http.Serve(s.listener, s.mux); err != nil {
			log.Printf("[WORKER-SERVER] Server error: %v", err)
		}
	}()
}

// GetEndpoint returns the full HTTP endpoint URL
func (s *Server) GetEndpoint() string {
	return s.endpoint
}

// Close closes the server
func (s *Server) Close() error {
	return s.listener.Close()
}

// getLocalIP returns the non-loopback local IP of the host
func getLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}

	return "", net.ErrClosed
}
