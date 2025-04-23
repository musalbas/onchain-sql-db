package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/musalbas/onchain-sql-db/pkg/celestia"
	"github.com/musalbas/onchain-sql-db/pkg/sql"
)

// Config represents the server configuration
type Config struct {
	Port            int
	DBPath          string
	CelestiaURL     string
	CelestiaToken   string
	CelestiaNameHex string
}

// Server represents the HTTP server for onchain-sql-db
type Server struct {
	httpServer      *http.Server
	sqlManager      *sql.Manager
	celestiaManager *celestia.Manager
	blockProcessor  *BlockProcessor
	queryQueue      *QueryQueue
	mutex           sync.Mutex
}

// NewServer creates a new server instance
func NewServer(cfg Config) (*Server, error) {
	// Create SQL manager
	sqlManager, err := sql.NewManager(cfg.DBPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create SQL manager: %w", err)
	}

	// Create Celestia manager
	celestiaManager, err := celestia.NewManager(
		cfg.CelestiaURL,
		cfg.CelestiaToken,
		cfg.CelestiaNameHex,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Celestia manager: %w", err)
	}

	// Create the query queue
	queryQueue := NewQueryQueue(celestiaManager, 2*time.Second)

	// Create the server instance
	server := &Server{
		sqlManager:      sqlManager,
		celestiaManager: celestiaManager,
		queryQueue:      queryQueue,
	}
	
	// Create and attach the block processor
	blockProcessor := NewBlockProcessor(sqlManager, celestiaManager)
	server.blockProcessor = blockProcessor

	// Create HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/query", server.handleQuery)
	mux.HandleFunc("/batch", server.handleBatch)
	mux.HandleFunc("/replay", server.handleReplay)
	mux.HandleFunc("/status", server.handleStatus)

	server.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: mux,
	}

	return server, nil
}

// Start starts the HTTP server, block processor, and query queue
func (s *Server) Start() error {
	// Start the query queue
	s.queryQueue.Start()

	// Start the block processor
	s.blockProcessor.Start()
	
	// Start the HTTP server
	return s.httpServer.ListenAndServe()
}

// Stop stops the HTTP server, block processor, and query queue
func (s *Server) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Stop the HTTP server
	if err := s.httpServer.Shutdown(ctx); err != nil {
		log.Printf("Failed to shutdown server: %v", err)
	}

	// Stop the query queue
	s.queryQueue.Stop()

	// Stop the block processor
	s.blockProcessor.Stop()

	// Close database
	if err := s.sqlManager.Close(); err != nil {
		log.Printf("Failed to close SQL manager: %v", err)
	}

	// Close Celestia client
	s.celestiaManager.Close()
}

// QueryRequest represents an SQL query request
type QueryRequest struct {
	Query string `json:"query"`
}

// BatchRequest represents a batch of SQL queries
type BatchRequest struct {
	Queries []string `json:"queries"`
}

// QueryResponse represents an SQL query response
type QueryResponse struct {
	Success bool            `json:"success"`
	Results [][]interface{} `json:"results,omitempty"`
	Height  uint64          `json:"height,omitempty"`
	Error   string          `json:"error,omitempty"`
	Message string          `json:"message,omitempty"`
}

// BatchResponse represents a response for a batch of queries
type BatchResponse struct {
	Success bool   `json:"success"`
	Height  uint64 `json:"height,omitempty"`
	Count   int    `json:"count"`
	Error   string `json:"error,omitempty"`
	Message string `json:"message,omitempty"`
}

// ReplayRequest represents a replay request
type ReplayRequest struct {
	FromHeight uint64 `json:"from_height"`
	ToHeight   uint64 `json:"to_height,omitempty"`
}

// StatusResponse represents the current status
type StatusResponse struct {
	CurrentHeight      uint64 `json:"current_height"`
	ProcessedHeight    uint64 `json:"processed_height"`
	ProcessedQueries   int    `json:"processed_queries"`
	CelestiaNamespace  string `json:"celestia_namespace"`
	CelestiaConnection bool   `json:"celestia_connection"`
}

// handleQuery handles SQL query requests
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if r.Method != http.MethodPut && r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var query string

	// Handle PUT request (write query to chain)
	if r.Method == http.MethodPut {
		var req QueryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		query = req.Query

		// Add the query to the queue without waiting for the result
		// This makes the API non-blocking while still batching queries
		s.queryQueue.AddQuery(query)
		
		// Return immediate success response without waiting for blockchain submission
		json.NewEncoder(w).Encode(QueryResponse{
			Success: true,
			Message: "Query added to submission queue and will be processed in batches",
		})
		return
	}

	// Handle GET request (read from database)
	query = r.URL.Query().Get("query")
	if query == "" {
		http.Error(w, "Missing query parameter", http.StatusBadRequest)
		return
	}

	results, err := s.sqlManager.ExecuteQuery(query)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to execute query: %v", err), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(QueryResponse{
		Success: true,
		Results: results,
	})
}

// handleReplay handles replaying queries from the blockchain
func (s *Server) handleReplay(w http.ResponseWriter, r *http.Request) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ReplayRequest

	// Parse request body if provided
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
	} else {
		// Parse query parameters
		fromHeightStr := r.URL.Query().Get("from_height")
		if fromHeightStr != "" {
			fromHeight, err := strconv.ParseUint(fromHeightStr, 10, 64)
			if err != nil {
				http.Error(w, "Invalid from_height parameter", http.StatusBadRequest)
				return
			}
			req.FromHeight = fromHeight
		}

		toHeightStr := r.URL.Query().Get("to_height")
		if toHeightStr != "" {
			toHeight, err := strconv.ParseUint(toHeightStr, 10, 64)
			if err != nil {
				http.Error(w, "Invalid to_height parameter", http.StatusBadRequest)
				return
			}
			req.ToHeight = toHeight
		}
	}

	if req.FromHeight == 0 {
		http.Error(w, "Missing from_height parameter", http.StatusBadRequest)
		return
	}

	// Reset database
	if err := s.sqlManager.Reset(); err != nil {
		http.Error(w, fmt.Sprintf("Failed to reset database: %v", err), http.StatusInternalServerError)
		return
	}

	// Replay queries
	toHeight := req.ToHeight
	if toHeight == 0 {
		// Get latest height
		var err error
		toHeight, err = s.celestiaManager.GetLatestHeight(r.Context())
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to get latest height: %v", err), http.StatusInternalServerError)
			return
		}
	}

	// Fetch and replay queries
	log.Printf("Replaying queries from height %d to %d", req.FromHeight, toHeight)
	
	replayResults := make(map[uint64]struct {
		Success bool
		Error   string
	})

	for height := req.FromHeight; height <= toHeight; height++ {
		queries, err := s.celestiaManager.GetQueriesAtHeight(r.Context(), height)
		if err != nil {
			log.Printf("Failed to get queries at height %d: %v", height, err)
			replayResults[height] = struct {
				Success bool
				Error   string
			}{
				Success: false,
				Error:   err.Error(),
			}
			continue
		}

		for _, query := range queries {
			_, err := s.sqlManager.ExecuteQuery(query)
			if err != nil {
				log.Printf("Failed to execute query at height %d: %v", height, err)
				replayResults[height] = struct {
					Success bool
					Error   string
				}{
					Success: false,
					Error:   err.Error(),
				}
			} else {
				replayResults[height] = struct {
					Success bool
					Error   string
				}{
					Success: true,
				}
			}
		}
	}

	json.NewEncoder(w).Encode(replayResults)
}

// handleBatch handles batch SQL query requests
func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse the batch request
	var req BatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate the request
	if len(req.Queries) == 0 {
		http.Error(w, "No queries provided", http.StatusBadRequest)
		return
	}

	// Add all queries to the queue at once without waiting for results
	for _, query := range req.Queries {
		s.queryQueue.AddQuery(query)
	}

	// Return immediate success response with batch info
	json.NewEncoder(w).Encode(BatchResponse{
		Success: true,
		Count:   len(req.Queries),
		Message: fmt.Sprintf("%d queries added to submission queue and will be processed in batches", len(req.Queries)),
	})
}

// handleStatus returns the current status of the node
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get latest height
	height, err := s.celestiaManager.GetLatestHeight(r.Context())
	if err != nil {
		log.Printf("Failed to get latest height: %v", err)
	}

	// Get processed queries count
	count, err := s.sqlManager.GetQueryCount()
	if err != nil {
		log.Printf("Failed to get query count: %v", err)
	}

	processedHeight := s.blockProcessor.GetLastProcessedHeight()
	
	resp := StatusResponse{
		CurrentHeight:      height,
		ProcessedHeight:    processedHeight,
		ProcessedQueries:   count,
		CelestiaNamespace:  s.celestiaManager.GetNamespace(),
		CelestiaConnection: s.celestiaManager.IsConnected(),
	}

	json.NewEncoder(w).Encode(resp)
}
