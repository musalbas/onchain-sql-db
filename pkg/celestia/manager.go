package celestia

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync/atomic"

	client "github.com/celestiaorg/celestia-openrpc"
	"github.com/celestiaorg/celestia-openrpc/types/blob"
	"github.com/celestiaorg/celestia-openrpc/types/share"
)

// Manager handles Celestia blockchain operations
type Manager struct {
	client    *client.Client
	namespace share.Namespace
	connected atomic.Bool
	url       string
	token     string
	nameHex   string
}

// NewManager creates a new Celestia manager
func NewManager(url, token, namespaceHex string) (*Manager, error) {
	// Convert hex namespace to bytes
	namespaceBytes, err := hex.DecodeString(namespaceHex)
	if err != nil {
		return nil, fmt.Errorf("invalid namespace hex: %w", err)
	}

	// Create namespace
	namespace, err := share.NewBlobNamespaceV0(namespaceBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to create namespace: %w", err)
	}

	// Create client
	ctx := context.Background()
	client, err := client.NewClient(ctx, url, token)
	if err != nil {
		// Return manager without connected client
		return &Manager{
			url:       url,
			token:     token,
			namespace: namespace,
			nameHex:   namespaceHex,
		}, nil
	}

	manager := &Manager{
		client:    client,
		namespace: namespace,
		url:       url,
		token:     token,
		nameHex:   namespaceHex,
	}
	manager.connected.Store(true)

	return manager, nil
}

// Close closes the Celestia client
func (m *Manager) Close() {
	if m.client != nil {
		m.client.Close()
		m.connected.Store(false)
	}
}

// IsConnected returns whether the manager is connected to Celestia
func (m *Manager) IsConnected() bool {
	return m.connected.Load()
}

// GetNamespace returns the Celestia namespace
func (m *Manager) GetNamespace() string {
	return m.nameHex
}

// SubmitQuery submits an SQL query to the blockchain
func (m *Manager) SubmitQuery(ctx context.Context, query string) (uint64, error) {
	if !m.connected.Load() {
		// Try to reconnect
		client, err := client.NewClient(ctx, m.url, m.token)
		if err != nil {
			return 0, fmt.Errorf("failed to connect to Celestia: %w", err)
		}
		m.client = client
		m.connected.Store(true)
	}

	// Create a blob with the query
	queryBlob, err := blob.NewBlobV0(m.namespace, []byte(query))
	if err != nil {
		return 0, fmt.Errorf("failed to create blob: %w", err)
	}

	// Submit the blob to the network
	height, err := m.client.Blob.Submit(ctx, []*blob.Blob{queryBlob}, blob.NewSubmitOptions())
	if err != nil {
		m.connected.Store(false)
		return 0, fmt.Errorf("failed to submit blob: %w", err)
	}

	return height, nil
}

// GetQueriesAtHeight fetches SQL queries stored at a specific height
func (m *Manager) GetQueriesAtHeight(ctx context.Context, height uint64) ([]string, error) {
	if !m.connected.Load() {
		// Try to reconnect
		client, err := client.NewClient(ctx, m.url, m.token)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to Celestia: %w", err)
		}
		m.client = client
		m.connected.Store(true)
	}

	// Fetch blobs from the network
	blobs, err := m.client.Blob.GetAll(ctx, height, []share.Namespace{m.namespace})
	if err != nil {
		m.connected.Store(false)
		return nil, fmt.Errorf("failed to fetch blobs: %w", err)
	}

	// Extract queries from blobs
	queries := make([]string, 0, len(blobs))
	for _, blob := range blobs {
		queries = append(queries, string(blob.Data))
	}

	return queries, nil
}

// GetLatestHeight fetches the latest block height
func (m *Manager) GetLatestHeight(ctx context.Context) (uint64, error) {
	if !m.connected.Load() {
		// Try to reconnect
		client, err := client.NewClient(ctx, m.url, m.token)
		if err != nil {
			return 0, fmt.Errorf("failed to connect to Celestia: %w", err)
		}
		m.client = client
		m.connected.Store(true)
	}

	// Get the latest block header
	header, err := m.client.Header.NetworkHead(ctx)
	if err != nil {
		m.connected.Store(false)
		return 0, fmt.Errorf("failed to get network head: %w", err)
	}

	return header.Height(), nil
}
