package server

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/musalbas/onchain-sql-db/pkg/celestia"
)

const (
	// MaxBatchSizeBytes represents maximum size of a batch in bytes (30MB)
	MaxBatchSizeBytes = 30 * 1024 * 1024
	// DefaultBatchInterval represents the default time to wait before processing a batch
	DefaultBatchInterval = 2 * time.Second
)

// QueryQueueItem represents a single item in the query queue
type QueryQueueItem struct {
	Query   string
	Size    int // Size in bytes
	AddedAt time.Time
	ResultChan chan QueryResult
}

// QueryResult represents the result of a queued query submission
type QueryResult struct {
	Success bool
	Height  uint64
	Error   error
}

// QueryQueue handles batching of SQL queries before submission to Celestia
type QueryQueue struct {
	celestiaManager *celestia.Manager
	queue           []QueryQueueItem
	mutex           sync.Mutex
	batchInterval   time.Duration
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
}

// NewQueryQueue creates a new query queue
func NewQueryQueue(celestiaManager *celestia.Manager, batchInterval time.Duration) *QueryQueue {
	if batchInterval <= 0 {
		batchInterval = DefaultBatchInterval
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &QueryQueue{
		celestiaManager: celestiaManager,
		queue:           make([]QueryQueueItem, 0),
		batchInterval:   batchInterval,
		ctx:             ctx,
		cancel:          cancel,
	}
}

// Start starts the query queue processor
func (q *QueryQueue) Start() {
	q.wg.Add(1)
	go q.processQueue()
}

// Stop stops the query queue processor
func (q *QueryQueue) Stop() {
	q.cancel()
	q.wg.Wait()
}

// AddQuery adds a query to the queue with option to wait for result
// If waitResult is true, it returns a channel that will receive the result
// If waitResult is false, it returns nil (non-blocking mode)
func (q *QueryQueue) AddQuery(query string) chan QueryResult {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	// Create a buffered channel to avoid blocking the sender if no one is receiving
	var resultChan chan QueryResult = nil
	
	// In non-blocking mode we still create a dummy channel for internal use
	// but we don't return it to the caller
	dummyChan := make(chan QueryResult, 1)
	
	// Add query to queue
	q.queue = append(q.queue, QueryQueueItem{
		Query:      query,
		Size:       len(query),
		AddedAt:    time.Now(),
		ResultChan: dummyChan,
	})

	// Log for monitoring purposes
	log.Printf("Added query to queue, current queue size: %d items", len(q.queue))
	
	return resultChan // Will be nil in non-blocking mode
}

// processQueue periodically processes the query queue
func (q *QueryQueue) processQueue() {
	defer q.wg.Done()

	ticker := time.NewTicker(q.batchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-q.ctx.Done():
			return
		case <-ticker.C:
			q.processBatch()
		}
	}
}

// processBatch processes a batch of queries from the queue
func (q *QueryQueue) processBatch() {
	q.mutex.Lock()
	
	// If queue is empty, nothing to do
	if len(q.queue) == 0 {
		q.mutex.Unlock()
		return
	}

	// Create batches respecting the max size limit
	var batches [][]QueryQueueItem
	var currentBatch []QueryQueueItem
	var currentBatchSize int

	for _, item := range q.queue {
		// If this item would exceed the batch size, start a new batch
		if currentBatchSize+item.Size > MaxBatchSizeBytes {
			if len(currentBatch) > 0 {
				batches = append(batches, currentBatch)
				currentBatch = []QueryQueueItem{}
				currentBatchSize = 0
			}
			
			// If a single item is larger than max batch size, we need to handle it separately
			// (this shouldn't happen with normal SQL queries, but better to be safe)
			if item.Size > MaxBatchSizeBytes {
				log.Printf("WARNING: Query exceeds max batch size (%d bytes)", item.Size)
				// Create a batch with just this item
				batches = append(batches, []QueryQueueItem{item})
				continue
			}
		}
		
		// Add item to current batch
		currentBatch = append(currentBatch, item)
		currentBatchSize += item.Size
	}
	
	// Add the last batch if it's not empty
	if len(currentBatch) > 0 {
		batches = append(batches, currentBatch)
	}
	
	// Clear the queue
	q.queue = []QueryQueueItem{}
	
	// Unlock before processing batches to avoid holding the lock during network operations
	q.mutex.Unlock()
	
	// Process each batch
	for _, batch := range batches {
		q.submitBatch(batch)
	}
}

// submitBatch submits a batch of queries to Celestia
func (q *QueryQueue) submitBatch(batch []QueryQueueItem) {
	if len(batch) == 0 {
		return
	}
	
	// Always encode queries as a JSON array, even for a single query
	batchQueries := make([]string, len(batch))
	for i, item := range batch {
		batchQueries[i] = item.Query
	}
	
	// Convert batch to JSON string
	batchJSON := fmt.Sprintf("[%s]", quoteAndJoin(batchQueries, ','))
	
	// Log the batch submission for debugging
	log.Printf("Submitting batch of %d queries (%.2f KB)", len(batch), float64(len(batchJSON))/1024)
	
	// Submit the batch
	height, err := q.celestiaManager.SubmitQuery(q.ctx, batchJSON)
	
	// Process result for each query in the batch
	for _, item := range batch {
		// Handle channels safely in case they're nil or closed
		if item.ResultChan != nil {
			if err != nil {
				item.ResultChan <- QueryResult{
					Success: false,
					Error:   err,
				}
			} else {
				item.ResultChan <- QueryResult{
					Success: true,
					Height:  height,
				}
			}
			// Close the channel to prevent resource leaks
			close(item.ResultChan)
		}
	}
}

// quoteAndJoin quotes each string in the slice and joins them with the specified separator
func quoteAndJoin(strs []string, sep rune) string {
	if len(strs) == 0 {
		return ""
	}
	
	var result string
	for i, s := range strs {
		if i > 0 {
			result += string(sep)
		}
		result += fmt.Sprintf("%q", s)
	}
	return result
}
