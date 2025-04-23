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
	// MaxBatchSizeBytes represents maximum size of a batch in bytes (1.88MB)
	// Set slightly below Celestia's limit of 1,974,272 bytes
	MaxBatchSizeBytes = 1_900_000
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
	queue           []QueryQueueItem   // Primary queue for incoming queries
	readyBatches    [][]QueryQueueItem // Batches ready for submission
	mutex           sync.Mutex
	batchInterval   time.Duration
	batchTicker     *time.Ticker    // Ticker for batch interval, can be reset
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	processing      bool           // Flag to indicate if a batch is currently being submitted
	processCond     *sync.Cond     // Condition variable for coordinating batch processing
	lastLogTime     time.Time      // Tracks when we last logged queue stats to limit frequency
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
		lastLogTime:     time.Now(),
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

	// Calculate total size before adding to check if we'll exceed max batch size
	totalQueueSize := 0
	for _, item := range q.queue {
		totalQueueSize += item.Size
	}
	
	// Add all queries to the queue and track size
	newQueriesSize := 0
	for i, query := range queries {
		resultChan := make(chan QueryResult, 1)
		resultChans[i] = resultChan
		querySize := len(query)
		newQueriesSize += querySize
		
		q.queue = append(q.queue, QueryQueueItem{
			Query:      query,
			Size:       querySize,
			AddedAt:    time.Now(),
			ResultChan: resultChan,
		})
	}
	
	// Update total queue size after adding the new queries
	totalQueueSize += newQueriesSize
	
	// Log queue size at most once per second
	triggerBatchNow := false
	if time.Since(q.lastLogTime) > time.Second {
		log.Printf("QUEUE STATUS: %d items, %.2f KB total (%.1f%% of max batch size)", 
			len(q.queue), float64(totalQueueSize)/1024, (float64(totalQueueSize)/float64(MaxBatchSizeBytes))*100)
		q.lastLogTime = time.Now()
	}
	
	// If queue exceeds max batch size, trigger batch creation immediately and reset timer
	if totalQueueSize >= MaxBatchSizeBytes {
		log.Printf("QUEUE FULL: Total size %.2f KB exceeds max batch size - triggering immediate batch creation", 
			float64(totalQueueSize)/1024)
		triggerBatchNow = true
		
		// Reset the batch interval timer
		q.resetBatchTimer()
	}
	
	// Signal that something has been added to the queue
	q.processCond.Signal()
	
	// Unlock before triggering batch to avoid deadlock
	q.mutex.Unlock()
	
	// If the queue is full, trigger batch creation immediately, but only for the full portion
	if triggerBatchNow {
		go q.createSizedBatch(MaxBatchSizeBytes)
	}
	
	return resultChans
}

// AddQuery adds a query to the queue and returns a channel that will receive the result
func (q *QueryQueue) AddQuery(query string) chan QueryResult {
	q.mutex.Lock()

	// Create result channel
	resultChan := make(chan QueryResult, 1)

	// Calculate total queue size before adding this query
	totalQueueSize := 0
	for _, item := range q.queue {
		totalQueueSize += item.Size
	}
	
	// Add the query to the queue
	querySize := len(query)
	q.queue = append(q.queue, QueryQueueItem{
		Query:      query,
		Size:       querySize,
		AddedAt:    time.Now(),
		ResultChan: resultChan,
	})
	
	// Update total queue size
	totalQueueSize += querySize
	
	// Log queue size at most once per second
	triggerBatchNow := false
	if time.Since(q.lastLogTime) > time.Second {
		log.Printf("QUEUE STATUS: %d items, %.2f KB total (%.1f%% of max batch size)", 
			len(q.queue), float64(totalQueueSize)/1024, (float64(totalQueueSize)/float64(MaxBatchSizeBytes))*100)
		q.lastLogTime = time.Now()
	}
	
	// If queue exceeds max batch size, trigger batch creation immediately and reset timer
	if totalQueueSize >= MaxBatchSizeBytes {
		log.Printf("QUEUE FULL: Total size %.2f KB exceeds max batch size - triggering immediate batch creation", 
			float64(totalQueueSize)/1024)
		triggerBatchNow = true
		
		// Reset the batch interval timer
		q.resetBatchTimer()
	}
	
	// Signal that something has been added to the queue
	q.processCond.Signal()
	
	// Unlock before triggering batch to avoid deadlock
	q.mutex.Unlock()
	
	// If the queue is full, trigger batch creation immediately, but only for the full portion
	if triggerBatchNow {
		go q.createSizedBatch(MaxBatchSizeBytes)
	}
	
	return resultChan
}

// processQueue periodically creates batches from the query queue
func (q *QueryQueue) processQueue() {
	defer q.wg.Done()

	ticker := time.NewTicker(q.batchInterval)
	defer ticker.Stop()
	log.Printf("Batch timer configured for %v intervals", q.batchInterval)

	// Store the ticker in a struct field so it can be reset
	q.mutex.Lock()
	q.batchTicker = ticker
	q.mutex.Unlock()

	for {
		select {
		case <-q.ctx.Done():
			return
		case <-ticker.C:
			log.Printf("BATCH TRIGGER: Timer interval of %v elapsed", q.batchInterval)
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
		log.Printf("BATCH INFO: No items in queue, skipping batch creation")
		return
	}

	// Calculate total size of all items in queue
	totalQueueSize := 0
	oldestItemTime := time.Now()
	for _, item := range q.queue {
		totalQueueSize += item.Size
		if item.AddedAt.Before(oldestItemTime) {
			oldestItemTime = item.AddedAt
		}
	}

	// We'll still log batch creation details, but only when batches are actually created
	if len(q.queue) > 0 {
		oldestItemAge := time.Since(oldestItemTime)
		log.Printf("BATCH PREPARE: Queue has %d items, total size %.2f KB, oldest item age: %v", 
			len(q.queue), float64(totalQueueSize)/1024, oldestItemAge)
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
		// Calculate and log size information for each batch
		for i, batch := range batches {
			batchSize := 0
			for _, item := range batch {
				batchSize += item.Size
			}
			log.Printf("BATCH DETAIL: Batch #%d contains %d items, size: %.2f KB (%.1f%% of max)", 
				i+1, len(batch), float64(batchSize)/1024, (float64(batchSize)/float64(MaxBatchSizeBytes))*100)
		}
		q.readyBatches = append(q.readyBatches, batches...)
		log.Printf("BATCH SUMMARY: Created %d new batches, total ready batches: %d", len(batches), len(q.readyBatches))
	} else {
		log.Printf("BATCH INFO: No batches created from queue of %d items", len(q.queue))
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

// resetBatchTimer resets the batch interval timer
func (q *QueryQueue) resetBatchTimer() {
	// Only reset if the ticker exists
	if q.batchTicker != nil {
		// Reset the ticker
		q.batchTicker.Reset(q.batchInterval)
		log.Printf("BATCH TIMER: Reset batch interval timer to %v", q.batchInterval)
	}
}

// createSizedBatch creates a batch up to the specified maximum size
func (q *QueryQueue) createSizedBatch(maxSizeBytes int) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	
	// If queue is empty, nothing to do
	if len(q.queue) == 0 {
		log.Printf("BATCH INFO: No items in queue, skipping sized batch creation")
		return
	}

	// Calculate total size of all items in queue
	totalQueueSize := 0
	oldestItemTime := time.Now()
	for _, item := range q.queue {
		totalQueueSize += item.Size
		if item.AddedAt.Before(oldestItemTime) {
			oldestItemTime = item.AddedAt
		}
	}

	oldestItemAge := time.Since(oldestItemTime)
	log.Printf("SIZED BATCH CREATE: Queue has %d items, total size %.2f KB, oldest item age: %v, max size: %.2f KB", 
		len(q.queue), float64(totalQueueSize)/1024, oldestItemAge, float64(maxSizeBytes)/1024)
	
	// Create a batch that respects the max size limit
	var batch []QueryQueueItem
	var batchSize int
	var remainingQueue []QueryQueueItem

	// Add items to the batch until we reach the max size
	for i, item := range q.queue {
		// If adding this item would exceed the batch size and we already have items in the batch
		if batchSize + item.Size > maxSizeBytes && len(batch) > 0 {
			// Add all remaining items (including current one) to remaining queue
			remainingQueue = append(remainingQueue, q.queue[i:]...)
			break
		}
		
		// Otherwise add item to the current batch
		batch = append(batch, item)
		batchSize += item.Size
		
		// If we've reached the max size, stop adding items
		if batchSize >= maxSizeBytes {
			// Add remaining items to queue
			if i < len(q.queue)-1 {
				remainingQueue = append(remainingQueue, q.queue[i+1:]...)
			}
			break
		}
	}

	// Update the queue to keep only remaining items
	q.queue = remainingQueue

	if len(batch) > 0 {
		// Log batch details
		log.Printf("SIZED BATCH CREATED: %d items, %.2f KB (%.1f%% of max), %d items remain in queue", 
			len(batch), float64(batchSize)/1024, (float64(batchSize)/float64(maxSizeBytes))*100, len(q.queue))
		
		// Add batch to ready batches
		q.readyBatches = append(q.readyBatches, batch)
	} else {
		log.Printf("SIZED BATCH SKIPPED: No suitable items found for batch creation")
	}
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
	batchSize := 0
	for _, item := range batch {
		batchSize += item.Size
	}
	log.Printf("BATCH SUBMIT: Submitting batch of %d items, size: %.2f KB (%.1f%% of max)", 
		len(batch), float64(batchSize)/1024, (float64(batchSize)/float64(MaxBatchSizeBytes))*100)
	
	submitStartTime := time.Now()
	err := q.submitBatch(batch)
	submitDuration := time.Since(submitStartTime)
	
	// Update processing state and signal waiting goroutines
	q.mutex.Lock()
	q.processing = false
	q.processCond.Broadcast() // Signal that processing is complete
	q.mutex.Unlock()
	
	if err != nil {
		log.Printf("BATCH ERROR: Failed to submit batch after %v: %v", submitDuration, err)
	} else {
		log.Printf("BATCH SUCCESS: Batch submitted successfully in %v", submitDuration)
	}
}
