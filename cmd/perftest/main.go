package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Configuration for the performance test
type Config struct {
	ServerURL       string
	NumRecords      int
	Concurrency     int
	BatchSize       int
	MaxTimeout      time.Duration
	PollInterval    time.Duration
	PollConcurrency int
	TestTableName   string
	VerboseLogging  bool
}

// QueryRequest represents a request to the query endpoint
type QueryRequest struct {
	Query string `json:"query"`
}

// QueryResponse represents a response from the query endpoint
type QueryResponse struct {
	Success bool        `json:"success"`
	Results [][]any     `json:"results,omitempty"`
	Height  uint64      `json:"height,omitempty"`
	Error   string      `json:"error,omitempty"`
	Message string      `json:"message,omitempty"`
}

// BatchRequest represents a batch of SQL queries to be executed
type BatchRequest struct {
	Queries []string `json:"queries"`
}

// BatchResponse represents a response from the batch endpoint
type BatchResponse struct {
	Success bool   `json:"success"`
	Count   int    `json:"count,omitempty"`
	Message string `json:"message,omitempty"`
	Error   string `json:"error,omitempty"`
}

// ServerStatus represents the status response of the server
type StatusResponse struct {
	Status             string `json:"status"`
	CurrentHeight      uint64 `json:"current_height"`
	ProcessedHeight    uint64 `json:"processed_height"`
	CelestiaConnection bool   `json:"celestia_connection"`
}

// Transaction record for tracking insert operations
type InsertRecord struct {
	ID                 int
	Value              string
	Height             uint64
	SubmitTime         time.Time
	Committed          bool
	CommitTime         time.Time
	CommitHeight       uint64
	RepresentsBatchSize int
}

// Transaction metrics
type Metrics struct {
	TotalRecords        int
	SubmittedRecords    int32
	CommittedRecords    int
	SubmissionStartTime time.Time
	SubmissionEndTime   time.Time
	CommitEndTime       time.Time
	Latencies           []time.Duration
	mutex               sync.Mutex
}

// Add a latency measurement to metrics
func (m *Metrics) AddLatency(d time.Duration) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Latencies = append(m.Latencies, d)
}

// Batch of records to insert
type InsertBatch struct {
	Records []*InsertRecord
}

func main() {
	// Parse command line flags
	config := Config{}
	flag.StringVar(&config.ServerURL, "server", "http://localhost:8080", "URL of the onchain-sql-db server")
	flag.IntVar(&config.NumRecords, "records", 1000, "Number of records to insert")
	flag.IntVar(&config.Concurrency, "concurrency", 10, "Number of concurrent workers for submitting transactions")
	flag.IntVar(&config.BatchSize, "batchsize", 10, "Number of inserts per batch")
	flag.IntVar(&config.PollConcurrency, "pollconcurrency", 20, "Number of concurrent workers for polling transaction status")
	flag.DurationVar(&config.MaxTimeout, "timeout", 10*time.Minute, "Maximum time to wait for completion")
	flag.DurationVar(&config.PollInterval, "pollinterval", 200*time.Millisecond, "How often to check for committed transactions")
	flag.StringVar(&config.TestTableName, "table", "perftest", "Name of test table to create")
	flag.BoolVar(&config.VerboseLogging, "verbose", false, "Enable verbose logging")
	flag.Parse()

	// Print configuration
	fmt.Printf("Performance Test Configuration:\n")
	fmt.Printf("- Server: %s\n", config.ServerURL)
	fmt.Printf("- Records: %d\n", config.NumRecords)
	fmt.Printf("- Concurrency: %d\n", config.Concurrency)
	fmt.Printf("- Batch Size: %d\n", config.BatchSize)
	fmt.Printf("- Poll Concurrency: %d\n", config.PollConcurrency)
	fmt.Printf("- Timeout: %v\n", config.MaxTimeout)
	fmt.Printf("- Poll Interval: %v\n", config.PollInterval)
	fmt.Printf("- Test Table: %s\n", config.TestTableName)
	fmt.Printf("- Verbose Logging: %v\n\n", config.VerboseLogging)

	// Check server connectivity
	fmt.Println("Checking server connectivity...")
	status, err := getServerStatus(config.ServerURL)
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	fmt.Printf("Server is online. Current height: %d, Processed height: %d\n",
		status.CurrentHeight, status.ProcessedHeight)
	if !status.CelestiaConnection {
		log.Fatalf("Server is not connected to Celestia. Cannot proceed with test.")
	}

	// Check if table exists (diagnostic only)
	fmt.Println("Checking if test table already exists...")
	verifyTableExists(config.ServerURL, config.TestTableName)

	// Create test table with IF NOT EXISTS
	fmt.Println("Creating test table (or ensuring it exists)...")
	if err := createTestTable(config.ServerURL, config.TestTableName); err != nil {
		log.Fatalf("Failed to create test table: %v", err)
	}
	fmt.Println("Test table created successfully.")

	// Initialize metrics
	metrics := &Metrics{
		TotalRecords:     config.NumRecords,
		SubmittedRecords: 0,
		CommittedRecords: 0,
		Latencies:        make([]time.Duration, 0, config.NumRecords),
	}

	// Prepare tracking structures
	records := make([]*InsertRecord, config.NumRecords)
	for i := 0; i < config.NumRecords; i++ {
		records[i] = &InsertRecord{
			ID:         i + 1,
			Value:      fmt.Sprintf("PerfTest-%d-%s", i+1, randomString(8)),
			SubmitTime: time.Time{},
			Height:     0,
			Committed:  false,
		}
	}

	// Set up signal handler for graceful termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nInterrupted. Generating report with partial results...")
		cancel()
	}()

	// Start the test
	fmt.Println("Starting performance test...")
	metrics.SubmissionStartTime = time.Now()

	// Create a waitgroup to track submissions
	var submissionWg sync.WaitGroup

	// Create batches
	batches := createBatches(records, config.BatchSize)
	fmt.Printf("Created %d batches of %d records each\n", len(batches), config.BatchSize)

	// Create channel for batches and results
	batchChan := make(chan *InsertBatch, len(batches))
	for _, batch := range batches {
		batchChan <- batch
	}
	close(batchChan)

	// Launch submission workers
	for i := 0; i < config.Concurrency; i++ {
		submissionWg.Add(1)
		go func(workerID int) {
			defer submissionWg.Done()
			for batch := range batchChan {
				if err := submitBatch(ctx, config.ServerURL, batch, config.TestTableName); err != nil {
					if ctx.Err() != nil {
						return // Context canceled
					}
					log.Printf("Worker %d: Failed to submit batch: %v", workerID, err)
				} else {
					atomic.AddInt32((*int32)(&metrics.SubmittedRecords), int32(len(batch.Records)))
					if config.VerboseLogging {
						log.Printf("Worker %d: Submitted batch of %d records", workerID, len(batch.Records))
					}
				}
			}
		}(i)
	}

	// Wait for all submissions to complete
	submissionDone := make(chan struct{})
	go func() {
		submissionWg.Wait()
		close(submissionDone)
	}()

	// Wait for submission completion or timeout
	select {
	case <-submissionDone:
		fmt.Println("All batches submitted successfully.")
	case <-ctx.Done():
		fmt.Println("Submission interrupted.")
	case <-time.After(config.MaxTimeout):
		fmt.Println("Submission timeout reached.")
	}

	metrics.SubmissionEndTime = time.Now()
	submissionTime := metrics.SubmissionEndTime.Sub(metrics.SubmissionStartTime)
	fmt.Printf("Submission phase completed in %v\n", submissionTime.Round(time.Millisecond))

	// Start monitoring for committed transactions
	fmt.Println("Monitoring for committed transactions...")

	// Ensure we have the latest processed height
	status, _ = getServerStatus(config.ServerURL)
	initialProcessedHeight := status.ProcessedHeight
	fmt.Printf("Current processed height: %d\n", initialProcessedHeight)
	
	// Debug: Print the height of submitted records
	recordsWithHeight := 0
	for _, record := range records {
		if record.Height > 0 {
			recordsWithHeight++
		}
	}
	fmt.Printf("Records with valid height: %d/%d\n", recordsWithHeight, len(records))

	// Set up context with timeout
	pollCtx, pollCancel := context.WithTimeout(ctx, config.MaxTimeout)
	defer pollCancel()

	// Start polling workers
	verificationDone := make(chan struct{}, 1) // Buffered channel to prevent blocking
	completedRecords := int32(0)

	go func() {
		// Give the system a moment to process transactions
		initialWait := 2 * time.Second
		fmt.Printf("Waiting %v for initial transaction processing...\n", initialWait)
		time.Sleep(initialWait)

		ticker := time.NewTicker(config.PollInterval)
		defer ticker.Stop()

		// Only check the last record from each batch instead of every record
		recordsToCheck := make([]*InsertRecord, 0, len(batches))
		for _, batch := range batches {
			if len(batch.Records) > 0 {
				// Get the last record from each batch
				lastRecord := batch.Records[len(batch.Records)-1]
				// Mark this record as representing the entire batch
				lastRecord.RepresentsBatchSize = len(batch.Records)
				recordsToCheck = append(recordsToCheck, lastRecord)
			}
		}

		if len(recordsToCheck) == 0 {
			fmt.Println("Warning: No records to verify. Verification cannot proceed.")
			return
		}

		fmt.Printf("Verifying %d submitted records...\n", len(recordsToCheck))

		// Use a pool of workers to check records in parallel
		for {
			select {
			case <-ticker.C:
				// Get current server status
				status, err := getServerStatus(config.ServerURL)
				if err != nil {
					log.Printf("Failed to get server status: %v", err)
					continue
				}

				// Debug information
				if config.VerboseLogging {
					log.Printf("Current height: %d, Processed height: %d", 
						status.CurrentHeight, status.ProcessedHeight)
				}

				// Create a channel for records to check
				recordChan := make(chan *InsertRecord, len(recordsToCheck))
				var checkWg sync.WaitGroup
				var newlyCommitted int32

				// Start poll workers
				for i := 0; i < config.PollConcurrency; i++ {
					checkWg.Add(1)
					go func(workerID int) {
						defer checkWg.Done()
						for record := range recordChan {
							if record.Committed || record.Height > status.ProcessedHeight {
								continue // Skip already committed or not-yet-processed records
							}

							// Check if record is committed
							if checkRecordCommitted(config.ServerURL, record, config.TestTableName) {
								record.Committed = true
								record.CommitTime = time.Now()
								record.CommitHeight = status.ProcessedHeight

								// Update metrics
								latency := record.CommitTime.Sub(record.SubmitTime)
								metrics.AddLatency(latency)
								
								// If this record represents a batch, count all records in the batch
								recordsRepresented := 1
								if record.RepresentsBatchSize > 0 {
									recordsRepresented = record.RepresentsBatchSize
								}
								
								atomic.AddInt32(&newlyCommitted, int32(recordsRepresented))
								newCompleted := atomic.AddInt32(&completedRecords, int32(recordsRepresented))

								if config.VerboseLogging {
									log.Printf("Record %d committed (latency: %v)", record.ID, latency.Round(time.Millisecond))
								}

								// Check if we're done
								if newCompleted >= int32(config.NumRecords) {
									// Signal completion, but don't close here
									// as the channel is closed in the outer scope
									select {
									case verificationDone <- struct{}{}:
										// Signal sent
									default:
										// Channel already has a value, no need to send
									}
									return
								}
							} else if config.VerboseLogging {
								log.Printf("Record %d not yet committed (height: %d)", record.ID, record.Height)
							}
						}
					}(i)
				}

				// Feed records to workers
				recordsQueued := 0
				for _, record := range recordsToCheck {
					if !record.Committed && record.Height > 0 && record.Height <= status.ProcessedHeight {
						recordChan <- record
						recordsQueued++
					}
				}
				close(recordChan)

				if config.VerboseLogging && recordsQueued > 0 {
					log.Printf("Queued %d records for verification", recordsQueued)
				}

				// Wait for this round of checks to complete
				checkWg.Wait()

				// Print progress if we found new committed records
				newlyCommittedCount := atomic.LoadInt32(&newlyCommitted)
				if newlyCommittedCount > 0 {
					committed := atomic.LoadInt32(&completedRecords)
					fmt.Printf("Progress: %d/%d records committed (%.1f%%)\n",
						committed, len(recordsToCheck),
						float64(committed)*100/float64(len(recordsToCheck)))
				}

				// Check if all records are committed
				if atomic.LoadInt32(&completedRecords) >= int32(config.NumRecords) {
					// Signal completion, but don't close here
					// as the channel might already be closed
					select {
					case verificationDone <- struct{}{}:
						// Signal sent
					default:
						// Channel already has a value, no need to send
					}
					return
				}

			case <-pollCtx.Done():
				fmt.Println("Verification timeout or interrupted.")
				return
			}
		}
	}()

	// Wait for all records to be verified or timeout
	select {
	case <-verificationDone:
		fmt.Println("All submitted records verified successfully!")
	case <-pollCtx.Done():
		fmt.Println("Verification phase interrupted or timed out.")
	}

	// Update metrics
	metrics.CommitEndTime = time.Now()
	committedRecords := int(atomic.LoadInt32(&completedRecords))
	// In case we represent more records than we actually submitted (due to batch markers)
	if committedRecords > config.NumRecords {
		committedRecords = config.NumRecords
	}
	metrics.CommittedRecords = committedRecords

	// Generate performance report
	generateReport(metrics, config)
}

// Create batches of records
func createBatches(records []*InsertRecord, batchSize int) []*InsertBatch {
	numBatches := (len(records) + batchSize - 1) / batchSize
	batches := make([]*InsertBatch, 0, numBatches)

	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}

		batch := &InsertBatch{
			Records: records[i:end],
		}
		batches = append(batches, batch)
	}

	return batches
}

// Create the test table in the database and wait for it to be confirmed
func createTestTable(serverURL, tableName string) error {
	// Construct query to create test table if it doesn't exist
	createTableSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id INTEGER PRIMARY KEY, value TEXT)`, tableName)

	// Send request to create table
	queryReq := QueryRequest{Query: createTableSQL}
	reqBody, err := json.Marshal(queryReq)
	if err != nil {
		return fmt.Errorf("failed to marshal create table request: %w", err)
	}

	resp, err := http.NewRequest(http.MethodPut, serverURL+"/query", bytes.NewBuffer(reqBody))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}
	resp.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	httpResp, err := client.Do(resp)
	if err != nil {
		return fmt.Errorf("failed to send create table request: %w", err)
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned non-OK status: %s", httpResp.Status)
	}

	var response QueryResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&response); err != nil {
		return fmt.Errorf("failed to decode create table response: %w", err)
	}

	if !response.Success {
		return fmt.Errorf("create table failed: %s", response.Error)
	}

	// Now wait for the table to be confirmed on-chain and available
	log.Println("Table creation query submitted, waiting for table to be confirmed...")
	
	// Set up a timeout context
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 120*time.Second) // Extended timeout
	defer cancel()
	
	// Check every second if the table is available
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-timeoutCtx.Done():
			return fmt.Errorf("timeout waiting for table %s to be confirmed", tableName)
		case <-ticker.C:
			// Verify the table exists
			err := verifyTableExists(serverURL, tableName)
			if err == nil {
				log.Printf("Table %s successfully created and verified!", tableName)
				return nil // Table exists, we can proceed
			}
			log.Printf("Waiting for table %s to be confirmed... (%v)", tableName, err)
		}
	}
}

// Submit a batch of inserts using the /batch endpoint
func submitBatch(ctx context.Context, serverURL string, batch *InsertBatch, tableName string) error {
	submitTime := time.Now()
	client := &http.Client{}
	
	// Get initial server status
	status, err := getServerStatus(serverURL)
	if err != nil {
		fmt.Printf("WARNING: Failed to get server status: %v\n", err)
	}
	
	currentHeight := uint64(0)
	if status != nil {
		currentHeight = status.CurrentHeight
	}
	
	// Create a batch request with all insert statements
	queries := make([]string, 0, len(batch.Records))
	
	for _, record := range batch.Records {
		// Create INSERT statement for this record
		insertSQL := fmt.Sprintf("INSERT INTO %s (id, value) VALUES (%d, '%s')",
			tableName, record.ID, record.Value)
		
		// Add to queries list
		queries = append(queries, insertSQL)
		
		// Set submission time for all records
		record.SubmitTime = submitTime
	}
	
	// Print batch summary
	fmt.Printf("INFO: Submitting batch with %d SQL statements\n", len(queries))
	
	// Send all queries in a single batch request
	batchReq := BatchRequest{Queries: queries}
	reqBody, err := json.Marshal(batchReq)
	if err != nil {
		return fmt.Errorf("failed to marshal batch request: %w", err)
	}
	
	// Create HTTP request to /batch endpoint
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, serverURL+"/batch", bytes.NewBuffer(reqBody))
	if err != nil {
		return fmt.Errorf("failed to create batch HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	
	// Send the batch request
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send batch request: %w", err)
	}
	
	// Check response
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return fmt.Errorf("server returned non-OK status for batch request: %s, body: %s", 
			resp.Status, string(respBody))
	}
	
	// Parse response
	var batchResponse BatchResponse
	if err := json.NewDecoder(resp.Body).Decode(&batchResponse); err != nil {
		resp.Body.Close()
		return fmt.Errorf("failed to decode batch response: %w", err)
	}
	resp.Body.Close()
	
	if !batchResponse.Success {
		return fmt.Errorf("batch submission failed: %s", batchResponse.Error)
	}
	
	// Print response details
	fmt.Printf("INFO: Batch response: success=%v, count=%d, message=%s\n", 
		batchResponse.Success, batchResponse.Count, batchResponse.Message)
	
	// Check server status again to get the height after batch submission
	newStatus, err := getServerStatus(serverURL)
	if err == nil && newStatus != nil && newStatus.CurrentHeight > currentHeight {
		currentHeight = newStatus.CurrentHeight
	}
	
	// Assign current tracking height to all records in the batch
	for _, record := range batch.Records {
		record.Height = currentHeight
	}
	
	// Log the height assignment for the batch
	fmt.Printf("DEBUG: Assigned height %d to all %d records in batch\n", 
		currentHeight, len(batch.Records))
	
	return nil
}

// Check if a record has been committed to the database
func checkRecordCommitted(serverURL string, record *InsertRecord, tableName string) bool {
	// Construct the SQL query
	query := fmt.Sprintf("SELECT id, value FROM %s WHERE id = %d", tableName, record.ID)
	
	// For GET requests, the API expects the query as a URL parameter
	url := fmt.Sprintf("%s/query?query=%s", serverURL, url.QueryEscape(query))
	fmt.Printf("DEBUG: Verification URL for record %d: %s\n", record.ID, url)
	
	// Send GET request
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("DEBUG: HTTP error checking record %d: %v\n", record.ID, err)
		return false
	}
	defer resp.Body.Close()
	
	respBody, _ := io.ReadAll(resp.Body)
	
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("DEBUG: Non-OK status for record %d: %s, body: %s\n", 
			record.ID, resp.Status, string(respBody))
		return false
	}
	
	// Debug output for the actual response
	fmt.Printf("DEBUG: Response for record %d: %s\n", record.ID, string(respBody))
	
	// Reset the response body for JSON decoding
	resp.Body = io.NopCloser(bytes.NewBuffer(respBody))
	
	var response QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		fmt.Printf("DEBUG: JSON decode error for record %d: %v\n", record.ID, err)
		return false
	}
	
	if !response.Success {
		fmt.Printf("DEBUG: Query failed for record %d: %s\n", record.ID, response.Error)
		return false
	}
	
	// Check if the record was found
	if len(response.Results) < 2 {
		fmt.Printf("DEBUG: No data rows for record %d (got %d rows)\n", record.ID, len(response.Results))
		return false
	}
	
	// Header row is at index 0, data row at index 1
	dataRow := response.Results[1]
	if len(dataRow) < 2 {
		fmt.Printf("DEBUG: Incomplete data row for record %d (columns: %d)\n", record.ID, len(dataRow))
		return false
	}
	
	// Check ID matches
	idValue, ok := dataRow[0].(float64)
	if !ok || int(idValue) != record.ID {
		fmt.Printf("DEBUG: ID mismatch for record %d (got %v)\n", record.ID, dataRow[0])
		return false
	}
	
	// Check value matches
	valueStr, ok := dataRow[1].(string)
	if !ok || valueStr != record.Value {
		fmt.Printf("DEBUG: Value mismatch for record %d (expected %s, got %v)\n", 
			record.ID, record.Value, dataRow[1])
		return false
	}
	
	fmt.Printf("DEBUG: Record %d successfully verified in database\n", record.ID)
	return true
}

// Verify that the test table exists
func verifyTableExists(serverURL, tableName string) error {
	// Try a simple SELECT query to see if the table exists
	query := fmt.Sprintf("SELECT name FROM sqlite_master WHERE type='table' AND name='%s'", tableName)

	// For GET requests, the API expects the query as a URL parameter
	url := fmt.Sprintf("%s/query?query=%s", serverURL, url.QueryEscape(query))
	
	// Send GET request
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("error checking table existence: %v", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned non-OK status: %s, body: %s", resp.Status, string(body))
	}
	
	// Parse the response
	var queryResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		return fmt.Errorf("failed to decode response: %v", err)
	}
	
	if !queryResp.Success {
		return fmt.Errorf("query failed: %s", queryResp.Error)
	}
	
	// Check if the table exists in the response
	if len(queryResp.Results) <= 1 { // Just header row or empty
		return fmt.Errorf("table %s not found in database", tableName)
	}
	
	// Table exists
	return nil
}

// Perform a test insert and verify it can be queried back
func performTestInsert(serverURL, tableName string) error {
	// Generate a unique test value
	testValue := fmt.Sprintf("test_%d", time.Now().UnixNano())
	
	// First, try to insert a test record with ID -1 (will be deleted later)
	insertSQL := fmt.Sprintf("INSERT OR REPLACE INTO %s (id, value) VALUES (-1, '%s')", tableName, testValue)
	
	// Send request to insert test record
	queryReq := QueryRequest{Query: insertSQL}
	reqBody, err := json.Marshal(queryReq)
	if err != nil {
		return fmt.Errorf("failed to marshal test insert request: %w", err)
	}
	
	req, err := http.NewRequest(http.MethodPut, serverURL+"/query", bytes.NewBuffer(reqBody))
	if err != nil {
		return fmt.Errorf("failed to create test insert HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	
	client := &http.Client{}
	httpResp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send test insert request: %w", err)
	}
	defer httpResp.Body.Close()
	
	if httpResp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned non-OK status for test insert: %s", httpResp.Status)
	}
	
	// Now wait a moment for the insert to be processed
	time.Sleep(2 * time.Second)
	
	// Try to query the test record back
	querySQL := fmt.Sprintf("SELECT id, value FROM %s WHERE id = -1", tableName)
	url := fmt.Sprintf("%s/query?query=%s", serverURL, url.QueryEscape(querySQL))
	
	// Send GET request
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to query test record: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned non-OK status for test query: %s", resp.Status)
	}
	
	// Parse the response
	var queryResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		return fmt.Errorf("failed to decode test query response: %w", err)
	}
	
	if !queryResp.Success {
		return fmt.Errorf("test query failed: %s", queryResp.Error)
	}
	
	// Check if the test record is found
	if len(queryResp.Results) <= 1 { // Just header row
		return fmt.Errorf("test record not found in table %s", tableName)
	}
	
	// Validate the returned value matches what we inserted
	if len(queryResp.Results) > 1 && len(queryResp.Results[1]) > 1 {
		valueObj := queryResp.Results[1][1]
		if value, ok := valueObj.(string); ok {
			if value == testValue {
				// Test successful - clean up by deleting the test record
				deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE id = -1", tableName)
				deleteReq := QueryRequest{Query: deleteSQL}
				reqBody, _ := json.Marshal(deleteReq)
				http.Post(serverURL+"/query", "application/json", bytes.NewBuffer(reqBody))
				
				return nil // Test passed
			}
			return fmt.Errorf("test value mismatch: expected '%s', got '%s'", testValue, value)
		}
	}
	
	return fmt.Errorf("invalid test query result format")
}

// Get server status
func getServerStatus(serverURL string) (*StatusResponse, error) {
	resp, err := http.Get(serverURL + "/status")
	if err != nil {
		return nil, fmt.Errorf("failed to get server status: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned non-OK status: %s", resp.Status)
	}
	
	var status StatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("failed to decode status response: %w", err)
	}
	
	return &status, nil
}

// Generate a detailed performance report
func generateReport(metrics *Metrics, config Config) {
	fmt.Println("\n-----------------------------------------")
	fmt.Println("         PERFORMANCE TEST REPORT         ")
	fmt.Println("-----------------------------------------")
	
	// Calculate statistics
	submissionTime := metrics.SubmissionEndTime.Sub(metrics.SubmissionStartTime)
	totalTime := metrics.CommitEndTime.Sub(metrics.SubmissionStartTime)
	
	fmt.Printf("Total records: %d\n", metrics.TotalRecords)
	fmt.Printf("Records submitted: %d (%.1f%%)\n", 
		int(metrics.SubmittedRecords), float64(metrics.SubmittedRecords)*100/float64(metrics.TotalRecords))
	fmt.Printf("Records committed: %d (%.1f%%)\n", 
		metrics.CommittedRecords, float64(metrics.CommittedRecords)*100/float64(metrics.TotalRecords))
	
	// Print timing information
	fmt.Printf("\nSubmission time: %v\n", submissionTime.Round(time.Millisecond))
	fmt.Printf("Total test time: %v\n", totalTime.Round(time.Millisecond))
	
	// Print throughput
	if int(metrics.SubmittedRecords) > 0 {
		fmt.Printf("Submission throughput: %.2f records/second\n", 
			float64(metrics.SubmittedRecords)/submissionTime.Seconds())
	}
	
	if metrics.CommittedRecords > 0 {
		fmt.Printf("End-to-end throughput: %.2f records/second\n", 
			float64(metrics.CommittedRecords)/totalTime.Seconds())
	}
	
	// Calculate latency percentiles if we have committed records
	if len(metrics.Latencies) > 0 {
		// Sort latencies
		sortedLatencies := make([]time.Duration, len(metrics.Latencies))
		copy(sortedLatencies, metrics.Latencies)
		
		// Simple insertion sort
		for i := 0; i < len(sortedLatencies); i++ {
			for j := i + 1; j < len(sortedLatencies); j++ {
				if sortedLatencies[i] > sortedLatencies[j] {
					sortedLatencies[i], sortedLatencies[j] = sortedLatencies[j], sortedLatencies[i]
				}
			}
		}
		
		// Calculate average
		var totalLatency time.Duration
		for _, l := range sortedLatencies {
			totalLatency += l
		}
		avgLatency := totalLatency / time.Duration(len(sortedLatencies))
		
		// Calculate percentiles
		p50 := sortedLatencies[len(sortedLatencies)*50/100]
		p90 := sortedLatencies[len(sortedLatencies)*90/100]
		p99 := sortedLatencies[len(sortedLatencies)*99/100]
		min := sortedLatencies[0]
		max := sortedLatencies[len(sortedLatencies)-1]
		
		fmt.Println("\nLatency statistics (submission to commit):")
		fmt.Printf("  Average: %v\n", avgLatency.Round(time.Millisecond))
		fmt.Printf("  Median (p50): %v\n", p50.Round(time.Millisecond))
		fmt.Printf("  p90: %v\n", p90.Round(time.Millisecond))
		fmt.Printf("  p99: %v\n", p99.Round(time.Millisecond))
		fmt.Printf("  Min: %v\n", min.Round(time.Millisecond))
		fmt.Printf("  Max: %v\n", max.Round(time.Millisecond))
	}
	
	fmt.Println("-----------------------------------------")
}

// Generate a random string of the specified length
func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// Initialize random
func init() {
	// For Go 1.20+ compatibility
	rand.New(rand.NewSource(time.Now().UnixNano()))
}
