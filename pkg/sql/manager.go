package sql

import (
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

// Manager handles SQL database operations
type Manager struct {
	db    *sql.DB
	path  string
	mutex sync.RWMutex
	queryCount int // Number of processed queries
}

// NewManager creates a new SQL manager with security restrictions
func NewManager(dbPath string) (*Manager, error) {
	// Use our secure connection with the SQLite authorizer
	db, err := GetSecureDBConnection(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open secure database: %w", err)
	}

	return &Manager{
		db:   db,
		path: dbPath,
	}, nil
}

// Close closes the database connection
func (m *Manager) Close() error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

// Reset resets the database by dropping all tables
func (m *Manager) Reset() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Get all tables
	rows, err := m.db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
	if err != nil {
		return fmt.Errorf("failed to get tables: %w", err)
	}
	defer rows.Close()

	// Drop each table
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("failed to scan table name: %w", err)
		}

		_, err := m.db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
		if err != nil {
			return fmt.Errorf("failed to drop table %s: %w", tableName, err)
		}
	}

	return nil
}

// Transaction runs a function within a transaction
func (m *Manager) Transaction(fn func(*sql.Tx) error) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	// Begin transaction
	tx, err := m.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	
	// Execute the function inside the transaction
	if err := fn(tx); err != nil {
		// Rollback on error
		tx.Rollback()
		return err
	}
	
	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	
	return nil
}

// ExecuteQueryWithTx executes an SQL query within a transaction and returns the results
func (m *Manager) ExecuteQueryWithTx(query string, tx *sql.Tx) ([][]interface{}, error) {
	// Track the number of queries
	m.queryCount++
	
	// Check if the query is a SELECT query or another type
	isSelect := false
	if len(query) >= 6 {
		upperQuery := query[:6]
		if upperQuery == "SELECT" || upperQuery == "select" {
			isSelect = true
		}
	}
	
	// Handle SELECT queries differently from other queries
	if isSelect {
		return m.executeSelectQueryWithTx(query, tx)
	}
	
	// For non-SELECT queries, just execute and return rows affected
	result, err := tx.Exec(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	
	rowsAffected, _ := result.RowsAffected()
	return [][]interface{}{{rowsAffected}}, nil
}

// executeSelectQueryWithTx executes a SELECT query within a transaction and returns the results
func (m *Manager) executeSelectQueryWithTx(query string, tx *sql.Tx) ([][]interface{}, error) {
	rows, err := tx.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute select query: %w", err)
	}
	defer rows.Close()
	
	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get column names: %w", err)
	}
	
	// Prepare result slice
	results := [][]interface{}{}
	
	// Add header row
	headerRow := make([]interface{}, len(columns))
	for i, column := range columns {
		headerRow[i] = column
	}
	results = append(results, headerRow)
	
	// Prepare value holders
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}
	
	// Iterate over rows
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		
		// Convert each value to a proper interface{}
		rowValues := make([]interface{}, len(columns))
		for i, val := range values {
			rowValues[i] = convertSQLiteValue(val)
		}
		
		results = append(results, rowValues)
	}
	
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during row iteration: %w", err)
	}
	
	return results, nil
}

// ExecuteQuery executes an SQL query and returns the results
func (m *Manager) ExecuteQuery(query string) ([][]interface{}, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Check if the query is a SELECT query or another type
	isSelect := false
	if len(query) >= 6 {
		upperQuery := query[:6]
		if upperQuery == "SELECT" || upperQuery == "select" {
			isSelect = true
		}
	}

	// Handle SELECT queries differently from other queries
	if isSelect {
		return m.executeSelectQuery(query)
	}

	// For non-SELECT queries, just execute and return rows affected
	result, err := m.db.Exec(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	return [][]interface{}{{rowsAffected}}, nil
}

// executeSelectQuery executes a SELECT query and returns the results
func (m *Manager) executeSelectQuery(query string) ([][]interface{}, error) {
	rows, err := m.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute select query: %w", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get column names: %w", err)
	}

	// Prepare result slice
	results := [][]interface{}{}

	// Add header row
	headerRow := make([]interface{}, len(columns))
	for i, column := range columns {
		headerRow[i] = column
	}
	results = append(results, headerRow)

	// Prepare value holders
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	// Iterate over rows
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert each value to a proper interface{}
		rowValues := make([]interface{}, len(columns))
		for i, val := range values {
			rowValues[i] = convertSQLiteValue(val)
		}

		results = append(results, rowValues)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during row iteration: %w", err)
	}

	return results, nil
}

// GetQueryCount returns the number of queries executed
func (m *Manager) GetQueryCount() (int, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.queryCount, nil
}

// convertSQLiteValue converts SQLite values to Go types
func convertSQLiteValue(val interface{}) interface{} {
	switch v := val.(type) {
	case []byte:
		return string(v)
	case nil:
		return nil
	default:
		return v
	}
}
