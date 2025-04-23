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
	// Set to 3s for more frequent batch processing while still being reasonable for Celestia
	DefaultBatchInterval = 3 * time.Second
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
	creatingBatch   bool           // Flag to indicate if we're currently creating a sized batch
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
		creatingBatch:   false,
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
	if time.Since(q.lastLogTime) > time.Second {
		log.Printf("QUEUE STATUS: %d items, %.2f KB total (%.1f%% of max batch size)", 
			len(q.queue), float64(totalQueueSize)/1024, (float64(totalQueueSize)/float64(MaxBatchSizeBytes))*100)
		q.lastLogTime = time.Now()
	}
	
	// Only trigger a batch if we're not already creating one
	triggerBatchNow := false
	
	// If queue exceeds max batch size and no batch is being created currently
	if totalQueueSize >= MaxBatchSizeBytes && !q.creatingBatch {
		log.Printf("QUEUE FULL: Total size %.2f KB exceeds max batch size - triggering immediate batch creation", 
			float64(totalQueueSize)/1024)
		
		triggerBatchNow = true
		
		// Reset the batch interval timer
		q.resetBatchTimer()
		
		// Set the flag to prevent multiple concurrent batch creations
		q.creatingBatch = true
	}
	
	// Signal that something has been added to the queue
	q.processCond.Signal()
	
	// Unlock before triggering batch to avoid deadlock
	q.mutex.Unlock()
	
	// If the queue is full and we're not already creating a batch, trigger batch creation immediately
	if triggerBatchNow {
		q.createSizedBatch(MaxBatchSizeBytes)
	}
	
	return resultChans
}

// AddQuery adds a query to the queue and returns a channel that will receive the result
func (q *QueryQueue) AddQuery(query string) chan QueryResult {
	q.mutex.Lock()

	// Create result channel
	resultChan := make(chan QueryResult, 1)

	// Calculate current queue size
	totalQueueSize := 0
	for _, item := range q.queue {
		totalQueueSize += item.Size
	}
	
	// Add query to queue
	querySize := len(query)
	q.queue = append(q.queue, QueryQueueItem{
		Query:      query,
		Size:       querySize,
		AddedAt:    time.Now(),
		ResultChan: resultChan,
	})
	
	// Update total queue size
	totalQueueSize += querySize
	
	// Log queue size (rate limited)
	triggerBatchNow := false
	if time.Since(q.lastLogTime) > time.Second {
		log.Printf("QUEUE STATUS: %d items, %.2f KB total (%.1f%% of max batch size)", 
			len(q.queue), float64(totalQueueSize)/1024, (float64(totalQueueSize)/float64(MaxBatchSizeBytes))*100)
		q.lastLogTime = time.Now()
	}
	
	// Only trigger a batch if we're not already creating one
	if totalQueueSize >= MaxBatchSizeBytes && !q.creatingBatch {
		log.Printf("QUEUE FULL: Total size %.2f KB exceeds max batch size - triggering immediate batch creation", 
			float64(totalQueueSize)/1024)
		triggerBatchNow = true
		
		// Set the flag to prevent multiple concurrent batch creations
		q.creatingBatch = true
		
		// Reset the batch interval timer
		q.resetBatchTimer()
	} else if totalQueueSize >= MaxBatchSizeBytes {
		log.Printf("QUEUE FULL: Total size %.2f KB exceeds max batch size but batch creation already in progress", 
			float64(totalQueueSize)/1024)
	}
	
	// Signal batch processor (even if not triggering now)
	q.processCond.Signal()
	
	q.mutex.Unlock()
	
	// If we decided to trigger a batch, do it without the lock held
	if triggerBatchNow {
		// Use direct call instead of goroutine to ensure sequential processing
		q.createSizedBatch(MaxBatchSizeBytes)
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
			// Check if we're already creating a batch before proceeding
			q.mutex.Lock()
			if q.creatingBatch {
				log.Printf("BATCH TIMER: Timer triggered but batch creation already in progress, skipping")
				q.mutex.Unlock()
				continue
			}
			// Set the flag to prevent concurrent batch creation
			q.creatingBatch = true
			q.mutex.Unlock()

			log.Printf("BATCH TRIGGER: Timer interval of %v elapsed", q.batchInterval)
			q.createBatch()
		}
	}
}

// createBatch creates a batch of queries from the queue based on timer interval
func (q *QueryQueue) createBatch() {
	q.mutex.Lock()
	
	// If queue is empty, nothing to do
	if len(q.queue) == 0 {
		log.Printf("BATCH INFO: No items in queue, skipping batch creation")
		q.creatingBatch = false
		q.mutex.Unlock()
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
	avgItemSize := float64(totalQueueSize) / float64(len(q.queue))
	
	log.Printf("TIMER BATCH: Queue has %d items, total size %.2f KB, oldest item age: %v, avg item size: %.0f bytes", 
		len(q.queue), float64(totalQueueSize)/1024, oldestItemAge, avgItemSize)
	
	// Check if we should create a sized batch instead of a timer-based batch
	if totalQueueSize > MaxBatchSizeBytes {
		log.Printf("BATCH SIZE EXCEEDED: Queue size %.2f KB exceeds max batch size (%.2f KB), using sized batch instead", 
			float64(totalQueueSize)/1024, float64(MaxBatchSizeBytes)/1024)
		
		// Release the lock before calling createSizedBatch
		q.mutex.Unlock()
		
		// Create a sized batch instead - createSizedBatch will handle the creatingBatch flag
		q.createSizedBatch(MaxBatchSizeBytes)
		return
	}
	
	// Create a timer-based batch with all items currently in the queue
	batch := q.queue
	
	// Calculate batch size for logging
	batchSize := 0
	for _, item := range batch {
		batchSize += item.Size
	}
	
	// Clear the queue
	q.queue = make([]QueryQueueItem, 0)
	
	// Log batch creation by timer
	log.Printf("TIMER BATCH CREATED: %d items, %.2f KB (%.1f%% of max), 0 items remain in queue", 
		len(batch), float64(batchSize)/1024, (float64(batchSize)/float64(MaxBatchSizeBytes))*100)
	
	// Add batch to ready batches
	q.readyBatches = append(q.readyBatches, batch)
	
	// Reset the batch interval timer
	q.resetBatchTimer()
	
	// Reset the creating batch flag before releasing the lock
	q.creatingBatch = false
	q.mutex.Unlock()
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
	
	// Note: We don't use defer q.mutex.Unlock() here because we want to ensure creatingBatch is always reset,
	// even if the function panics (which it shouldn't, but better to be safe)
	
	// If queue is empty, nothing to do
	if len(q.queue) == 0 {
		log.Printf("BATCH INFO: No items in queue, skipping sized batch creation")
		q.creatingBatch = false
		q.mutex.Unlock()
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
		
		// Reset the timer to prevent another batch from being created immediately
		q.resetBatchTimer()
	} else {
		log.Printf("SIZED BATCH SKIPPED: No suitable items found for batch creation")
	}
	
	// Always reset the creating batch flag when we're done
	q.creatingBatch = false
	q.mutex.Unlock()
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
