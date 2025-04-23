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
	mutex sync.Mutex
}

// NewManager creates a new SQL manager
func NewManager(dbPath string) (*Manager, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Test database connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to database: %w", err)
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
// Note: This is a placeholder implementation as we don't track query count yet
func (m *Manager) GetQueryCount() (int, error) {
	// For now, just return the number of tables as a proxy
	var count int
	err := m.db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count tables: %w", err)
	}
	return count, nil
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
