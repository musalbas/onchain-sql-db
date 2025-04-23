package celestia

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	client "github.com/celestiaorg/celestia-openrpc"
	"github.com/celestiaorg/celestia-openrpc/types/blob"
	"github.com/celestiaorg/celestia-openrpc/types/share"
)

// Manager handles Celestia blockchain operations
type Manager struct {
	client          *client.Client
	namespace       string
	connected       bool
	mutex           sync.RWMutex
	queryBlockCache map[uint64][]string // Cache of queries by block height
	cacheExpiry     time.Duration
	cacheLastUpdate map[uint64]time.Time
}

// NewManager creates a new Celestia manager
func NewManager(url, token, namespaceHex string) (*Manager, error) {
	// Validate the namespace hex
	_, err := hex.DecodeString(namespaceHex)
	if err != nil {
		return nil, fmt.Errorf("invalid namespace hex: %w", err)
	}

	// Create client
	ctx := context.Background()
	celestiaClient, err := client.NewClient(ctx, url, token)
	if err != nil {
		// Return manager without connected client
		return &Manager{
			client:          nil,
			namespace:       namespaceHex,
			connected:       false,
			mutex:           sync.RWMutex{},
			queryBlockCache: make(map[uint64][]string),
			cacheExpiry:     5 * time.Minute,
			cacheLastUpdate: make(map[uint64]time.Time),
		}, nil
	}

	manager := &Manager{
		client:          celestiaClient,
		namespace:       namespaceHex,
		connected:       true,
		mutex:           sync.RWMutex{},
		queryBlockCache: make(map[uint64][]string),
		cacheExpiry:     5 * time.Minute,
		cacheLastUpdate: make(map[uint64]time.Time),
	}

	return manager, nil
}

// Close closes the Celestia client
func (m *Manager) Close() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Clear the cache
	m.queryBlockCache = make(map[uint64][]string)
	m.cacheLastUpdate = make(map[uint64]time.Time)

	if m.client != nil {
		m.client.Close()
		m.connected = false
	}
}

// IsConnected returns whether the manager is connected to Celestia
func (m *Manager) IsConnected() bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.connected
}

// GetNamespace returns the Celestia namespace
func (m *Manager) GetNamespace() string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.namespace
}

// SubmitQuery submits an SQL query to the blockchain
func (m *Manager) SubmitQuery(ctx context.Context, query string) (uint64, error) {
	// Convert hex namespace to bytes
	namespaceBytes, err := hex.DecodeString(m.namespace)
	if err != nil {
		return 0, fmt.Errorf("invalid namespace hex: %w", err)
	}

	// Create namespace
	namespace, err := share.NewBlobNamespaceV0(namespaceBytes)
	if err != nil {
		return 0, fmt.Errorf("failed to create namespace: %w", err)
	}

	// Create a blob with the query
	queryBlob, err := blob.NewBlobV0(namespace, []byte(query))
	if err != nil {
		return 0, fmt.Errorf("failed to create blob: %w", err)
	}

	// Submit the blob to the network
	height, err := m.client.Blob.Submit(ctx, []*blob.Blob{queryBlob}, blob.NewSubmitOptions())
	if err != nil {
		m.mutex.Lock()
		m.connected = false
		m.mutex.Unlock()
		return 0, fmt.Errorf("failed to submit blob: %w", err)
	}

	return height, nil
}

// GetQueriesAtHeight retrieves all queries at a specific block height with caching
func (m *Manager) GetQueriesAtHeight(ctx context.Context, height uint64) ([]string, error) {
	// Check cache first
	m.mutex.RLock()
	queries, exists := m.queryBlockCache[height]
	updateTime, _ := m.cacheLastUpdate[height]
	m.mutex.RUnlock()

	// If we have a valid cache entry, return it
	if exists && time.Since(updateTime) < m.cacheExpiry {
		return queries, nil
	}

	// Convert hex namespace to bytes
	namespaceBytes, err := hex.DecodeString(m.namespace)
	if err != nil {
		return nil, fmt.Errorf("invalid namespace hex: %w", err)
	}

	// Create namespace
	namespaceDef, err := share.NewBlobNamespaceV0(namespaceBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to create namespace: %w", err)
	}

	// Fetch blobs from the network
	blobs, err := m.client.Blob.GetAll(ctx, height, []share.Namespace{namespaceDef})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch blobs: %w", err)
	}

	// Extract queries from blobs
	queryList := make([]string, 0, len(blobs))
	for _, b := range blobs {
		queryList = append(queryList, string(b.Data))
	}

	// Update cache
	m.mutex.Lock()
	m.queryBlockCache[height] = queryList
	m.cacheLastUpdate[height] = time.Now()
	m.mutex.Unlock()

	return queryList, nil
}

// GetLatestHeight fetches the latest block height
func (m *Manager) GetLatestHeight(ctx context.Context) (uint64, error) {
	header, err := m.client.Header.NetworkHead(ctx)
	if err != nil {
		m.mutex.Lock()
		m.connected = false
		m.mutex.Unlock()
		return 0, fmt.Errorf("failed to get network head: %w", err)
	}

	m.mutex.Lock()
	m.connected = true
	m.mutex.Unlock()
	return header.Height(), nil
}

// GetBatchQueriesInRange retrieves all queries within a range of block heights
// This is optimized for batch processing multiple blocks at once
func (m *Manager) GetBatchQueriesInRange(ctx context.Context, startHeight, endHeight uint64) (map[uint64][]string, error) {
	if startHeight > endHeight {
		return nil, fmt.Errorf("start height must be less than or equal to end height")
	}

	// Create result map
	results := make(map[uint64][]string)

	// Convert hex namespace to bytes
	namespaceBytes, err := hex.DecodeString(m.namespace)
	if err != nil {
		return nil, fmt.Errorf("invalid namespace hex: %w", err)
	}

	// Create namespace
	namespaceDef, err := share.NewBlobNamespaceV0(namespaceBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to create namespace: %w", err)
	}

	// Create a wait group for parallel processing
	var wg sync.WaitGroup
	// Create a mutex to protect the results map
	var resultsMutex sync.Mutex
	// Create an error channel
	errCh := make(chan error, endHeight-startHeight+1)

	// Process each height in parallel with a limit of 10 concurrent requests
	semaphore := make(chan struct{}, 10)

	for height := startHeight; height <= endHeight; height++ {
		// Skip heights that are already in cache and not expired
		m.mutex.RLock()
		queries, exists := m.queryBlockCache[height]
		updateTime, _ := m.cacheLastUpdate[height]
		cacheValid := exists && time.Since(updateTime) < m.cacheExpiry
		m.mutex.RUnlock()

		if cacheValid {
			resultsMutex.Lock()
			results[height] = queries
			resultsMutex.Unlock()
			continue
		}

		// Acquire semaphore slot
		semaphore <- struct{}{}

		// Process each height
		wg.Add(1)
		go func(h uint64) {
			defer wg.Done()
			defer func() { <-semaphore }()

			// Fetch blobs from the network
			blobs, err := m.client.Blob.GetAll(ctx, h, []share.Namespace{namespaceDef})
			if err != nil {
				errCh <- fmt.Errorf("failed to get blobs at height %d: %w", h, err)
				return
			}

			// Extract queries from blobs
			queries := make([]string, 0, len(blobs))
			for _, b := range blobs {
				queries = append(queries, string(b.Data))
			}

			// Update results
			resultsMutex.Lock()
			results[h] = queries
			resultsMutex.Unlock()

			// Update cache
			m.mutex.Lock()
			m.queryBlockCache[h] = queries
			m.cacheLastUpdate[h] = time.Now()
			m.mutex.Unlock()
		}(height)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Check if there were any errors
	select {
	case err := <-errCh:
		return results, err
	default:
		return results, nil
	}
}
