package server

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/musalbas/onchain-sql-db/pkg/celestia"
	sqlmanager "github.com/musalbas/onchain-sql-db/pkg/sql"
)

// BlockProcessor processes blocks from Celestia and executes queries
type BlockProcessor struct {
	sqlManager      *sqlmanager.Manager
	celestiaManager *celestia.Manager
	lastHeight      uint64
	batchSize       int
	pollInterval    time.Duration
	mutex           sync.RWMutex
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
}

// NewBlockProcessor creates a new block processor
func NewBlockProcessor(sqlManager *sqlmanager.Manager, celestiaManager *celestia.Manager) *BlockProcessor {
	ctx, cancel := context.WithCancel(context.Background())
	return &BlockProcessor{
		sqlManager:      sqlManager,
		celestiaManager: celestiaManager,
		lastHeight:      0,
		batchSize:       100,  // Process up to 100 blocks at a time for high throughput
		pollInterval:    500 * time.Millisecond, // Poll frequently for low latency
		ctx:             ctx,
		cancel:          cancel,
	}
}

// Start starts the block processor
func (p *BlockProcessor) Start() {
	log.Println("Starting block processor")
	p.wg.Add(1)
	go p.processBlocks()
}

// Stop stops the block processor
func (p *BlockProcessor) Stop() {
	log.Println("Stopping block processor")
	p.cancel()
	p.wg.Wait()
	log.Println("Block processor stopped")
}

// GetLastProcessedHeight returns the last processed block height
func (p *BlockProcessor) GetLastProcessedHeight() uint64 {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.lastHeight
}

// SetLastProcessedHeight sets the last processed block height
func (p *BlockProcessor) SetLastProcessedHeight(height uint64) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.lastHeight = height
}

// processBlocks processes blocks from Celestia
func (p *BlockProcessor) processBlocks() {
	defer p.wg.Done()

	// Initialize last height if not set
	if p.lastHeight == 0 {
		// Try to get the latest height as a starting point
		height, err := p.celestiaManager.GetLatestHeight(p.ctx)
		if err != nil {
			log.Printf("Failed to get latest height, starting from height 1: %v", err)
			p.lastHeight = 1
		} else {
			// Start from the latest height we know about
			p.lastHeight = height
			log.Printf("Starting block processor from height %d", p.lastHeight)
		}
	}

	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			if err := p.processNewBlocks(); err != nil {
				log.Printf("Error processing blocks: %v", err)
			}
		}
	}
}

// processNewBlocks processes any new blocks using batch processing for performance
func (p *BlockProcessor) processNewBlocks() error {
	// Get latest height
	latestHeight, err := p.celestiaManager.GetLatestHeight(p.ctx)
	if err != nil {
		return fmt.Errorf("failed to get latest height: %w", err)
	}

	// No new blocks
	if latestHeight <= p.lastHeight {
		return nil
	}

	// Determine range of blocks to process
	startHeight := p.lastHeight + 1
	endHeight := latestHeight

	// Limit the number of blocks processed in one batch
	if endHeight-startHeight+1 > uint64(p.batchSize) {
		endHeight = startHeight + uint64(p.batchSize) - 1
	}

	log.Printf("Processing blocks from height %d to %d", startHeight, endHeight)

	// Get all queries for the batch of blocks at once
	// This is much more efficient than querying one block at a time
	batchQueries, err := p.celestiaManager.GetBatchQueriesInRange(p.ctx, startHeight, endHeight)
	if err != nil {
		log.Printf("Error in batch query retrieval, falling back to individual block processing: %v", err)
		// Fall back to processing blocks one by one if batch retrieval fails
		return p.processBlocksSequentially(startHeight, endHeight)
	}

	// Begin a transaction for batch processing
	err = p.sqlManager.Transaction(func(tx *sql.Tx) error {
		// Process each block in sequential order
		for height := startHeight; height <= endHeight; height++ {
			// Get queries for this height from the batch result
			queries, exists := batchQueries[height]
			
			// If there are queries, execute them
			if exists && len(queries) > 0 {
				log.Printf("Processing %d queries at height %d", len(queries), height)
				for _, queryStr := range queries {
					// Check if this is a batched query and parse it if so
					parsedQueries, err := celestia.ParseBatchedQueries(queryStr)
					if err != nil {
						log.Printf("Error parsing batched query at height %d: %v", height, err)
						continue
					}
					
					// Execute each query in the batch
					for _, query := range parsedQueries {
						if _, err := p.sqlManager.ExecuteQueryWithTx(query, tx); err != nil {
							log.Printf("Error executing query at height %d: %v", height, err)
							// Continue processing even if a query fails
							// This ensures we don't get stuck on a bad query
						}
					}
				}
			}

			// Update the last processed height after each block
			p.SetLastProcessedHeight(height)
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("transaction error: %w", err)
	}

	return nil
}

// processBlocksSequentially is a fallback method that processes blocks one by one
// when batch query retrieval fails
func (p *BlockProcessor) processBlocksSequentially(startHeight, endHeight uint64) error {
	log.Printf("Falling back to sequential block processing from %d to %d", startHeight, endHeight)
	
	// Begin a transaction for batch processing
	err := p.sqlManager.Transaction(func(tx *sql.Tx) error {
		// Process each block
		for height := startHeight; height <= endHeight; height++ {
			// Get all queries at this height
			queries, err := p.celestiaManager.GetQueriesAtHeight(p.ctx, height)
			if err != nil {
				return fmt.Errorf("failed to get queries at height %d: %w", height, err)
			}

			// If there are queries, execute them
			if len(queries) > 0 {
				log.Printf("Processing %d queries at height %d", len(queries), height)
				for _, queryStr := range queries {
					// Check if this is a batched query and parse it if so
					parsedQueries, err := celestia.ParseBatchedQueries(queryStr)
					if err != nil {
						log.Printf("Error parsing batched query at height %d: %v", height, err)
						continue
					}
					
					// Execute each query in the batch
					for _, query := range parsedQueries {
						if _, err := p.sqlManager.ExecuteQueryWithTx(query, tx); err != nil {
							log.Printf("Error executing query at height %d: %v", height, err)
							// Continue processing even if a query fails
							// This ensures we don't get stuck on a bad query
						}
					}
				}
			}

			// Update the last processed height after each block
			p.SetLastProcessedHeight(height)
		}
		return nil
	})
	
	if err != nil {
		return fmt.Errorf("transaction error during sequential processing: %w", err)
	}
	
	return nil
}
