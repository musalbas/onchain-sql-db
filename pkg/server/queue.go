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
	// Set to 5s to be closer to Celestia's 6s block time for better synchronization
	DefaultBatchInterval = 5 * time.Second
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
	queue           []QueryQueueItem    // Primary queue for incoming queries
	readyBatches    [][]QueryQueueItem  // Batches ready for submission
	mutex           sync.Mutex
	batchInterval   time.Duration
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	processing      bool           // Flag to indicate if a batch is currently being submitted
	processCond     *sync.Cond     // Condition variable for coordinating batch processing
}

// NewQueryQueue creates a new query queue
func NewQueryQueue(celestiaManager *celestia.Manager, batchInterval time.Duration) *QueryQueue {
	if batchInterval <= 0 {
		batchInterval = DefaultBatchInterval
	}

	ctx, cancel := context.WithCancel(context.Background())
	
	q := &QueryQueue{
		celestiaManager: celestiaManager,
		queue:           make([]QueryQueueItem, 0),
		readyBatches:    make([][]QueryQueueItem, 0),
		batchInterval:   batchInterval,
		ctx:             ctx,
		cancel:          cancel,
		processing:      false,
	}
	
	// Initialize condition variable with queue mutex
	q.processCond = sync.NewCond(&q.mutex)
	
	return q
}

// Start starts the query queue processor
func (q *QueryQueue) Start() {
	// Start the batch creator
	q.wg.Add(1)
	go q.processQueue()
	
	// Start the batch submitter
	q.wg.Add(1)
	go q.submitBatches()
}

// Stop stops the query queue processor
func (q *QueryQueue) Stop() {
	q.cancel()
	q.wg.Wait()
}

// AddQueries adds multiple queries to the queue
func (q *QueryQueue) AddQueries(queries []string) []chan QueryResult {
	q.mutex.Lock()

	// Create result channels
	resultChans := make([]chan QueryResult, len(queries))

	// Add all queries to the queue - never block on processing
	for i, query := range queries {
		resultChan := make(chan QueryResult, 1)
		resultChans[i] = resultChan

		q.queue = append(q.queue, QueryQueueItem{
			Query:      query,
			Size:       len(query),
			AddedAt:    time.Now(),
			ResultChan: resultChan,
		})
	}

	// Log for monitoring purposes
	log.Printf("Added %d queries to queue, current queue size: %d items", len(queries), len(q.queue))

	q.mutex.Unlock()
	return resultChans
}

// AddQuery adds a query to the queue and returns a channel that will receive the result
func (q *QueryQueue) AddQuery(query string) chan QueryResult {
	q.mutex.Lock()

	// Create result channel
	resultChan := make(chan QueryResult, 1)

	// Add the query to the queue - never block on processing
	q.queue = append(q.queue, QueryQueueItem{
		Query:      query,
		Size:       len(query),
		AddedAt:    time.Now(),
		ResultChan: resultChan,
	})

	// Log for monitoring purposes
	log.Printf("Added query to queue, current queue size: %d items", len(q.queue))

	q.mutex.Unlock()
	return resultChan
}

// processQueue periodically creates batches from the query queue
func (q *QueryQueue) processQueue() {
	defer q.wg.Done()

	ticker := time.NewTicker(q.batchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-q.ctx.Done():
			return
		case <-ticker.C:
			q.createBatch()
		}
	}
}

// createBatch creates a batch of queries from the queue
func (q *QueryQueue) createBatch() {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	
	// If queue is empty, nothing to do
	if len(q.queue) == 0 {
		return
	}

	log.Printf("Creating new batch from queue of %d items", len(q.queue))
	
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
	
	// Add new batches to the ready queue
	if len(batches) > 0 {
		q.readyBatches = append(q.readyBatches, batches...)
		log.Printf("Created %d new batches, total ready batches: %d", len(batches), len(q.readyBatches))
	}
}

// submitBatch submits a single batch of queries to Celestia
func (q *QueryQueue) submitBatch(batch []QueryQueueItem) error {
	if len(batch) == 0 {
		return nil
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
	
	return err
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

// submitBatches runs in the background and submits ready batches in sequence
func (q *QueryQueue) submitBatches() {
	defer q.wg.Done()
	
	// Check for batches to submit every second
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-q.ctx.Done():
			return
		case <-ticker.C:
			q.processBatchQueue()
		}
	}
}

// processBatchQueue processes the next batch in the queue if available
func (q *QueryQueue) processBatchQueue() {
	q.mutex.Lock()
	
	// If no batches or already processing, nothing to do
	if len(q.readyBatches) == 0 || q.processing {
		q.mutex.Unlock()
		return
	}
	
	// Mark as processing and get the next batch
	q.processing = true
	batch := q.readyBatches[0]
	q.readyBatches = q.readyBatches[1:]
	
	// Unlock before submitting to avoid holding the lock during network operations
	q.mutex.Unlock()
	
	// Submit the batch
	err := q.submitBatch(batch)
	
	// Update processing state and signal waiting goroutines
	q.mutex.Lock()
	q.processing = false
	q.processCond.Broadcast() // Signal that processing is complete
	q.mutex.Unlock()
	
	if err != nil {
		log.Printf("Error submitting batch: %v", err)
	}
}
