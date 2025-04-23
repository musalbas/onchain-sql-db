package sql

import (
	"database/sql"
	"fmt"
	"log"

	sqlite3 "github.com/mattn/go-sqlite3"
)

// SQLite authorization action codes
// These constants are from sqlite3.h
const (
	SQLITE_CREATE_INDEX        = 1
	SQLITE_CREATE_TABLE        = 2
	SQLITE_CREATE_TEMP_INDEX   = 3
	SQLITE_CREATE_TEMP_TABLE   = 4
	SQLITE_CREATE_TEMP_TRIGGER = 5
	SQLITE_CREATE_TEMP_VIEW    = 6
	SQLITE_CREATE_TRIGGER      = 7
	SQLITE_CREATE_VIEW         = 8
	SQLITE_DELETE              = 9
	SQLITE_DROP_INDEX          = 10
	SQLITE_DROP_TABLE          = 11
	SQLITE_DROP_TEMP_INDEX     = 12
	SQLITE_DROP_TEMP_TABLE     = 13
	SQLITE_DROP_TEMP_TRIGGER   = 14
	SQLITE_DROP_TEMP_VIEW      = 15
	SQLITE_DROP_TRIGGER        = 16
	SQLITE_DROP_VIEW           = 17
	SQLITE_INSERT              = 18
	SQLITE_PRAGMA              = 19
	SQLITE_READ                = 20
	SQLITE_SELECT              = 21
	SQLITE_TRANSACTION         = 22
	SQLITE_UPDATE              = 23
	SQLITE_ATTACH              = 24
	SQLITE_DETACH              = 25
	SQLITE_ALTER_TABLE         = 26
	SQLITE_REINDEX             = 27
	SQLITE_ANALYZE             = 28
	SQLITE_CREATE_VTABLE       = 29
	SQLITE_DROP_VTABLE         = 30
	SQLITE_FUNCTION            = 31
	SQLITE_SAVEPOINT           = 32
	SQLITE_RECURSIVE           = 33
)

// SQLite authorizer return codes
const (
	SQLITE_OK     = 0 // Operation is allowed
	SQLITE_DENY   = 1 // Operation is denied
	SQLITE_IGNORE = 2 // Column value is NULL
)

// Register a secure SQLite driver with our authorizer
func init() {
	// Register a custom SQLite driver with an authorizer
	sql.Register("sqlite3_secure", &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			// Register our authorizer function that will restrict operations
			conn.RegisterAuthorizer(secureAuthorizer)
			return nil
		},
	})
}

// secureAuthorizer is the SQLite authorization callback that restricts operations
// to only SELECT, INSERT, and CREATE TABLE
//
// Parameters:
//   - action: The action code (what operation is being attempted)
//   - arg1: Usually the name of the table or index being operated on
//   - arg2: Usually the name of the column being operated on
//   - dbName: The database name
//
// Returns:
//   - SQLITE_OK if the operation is allowed
//   - SQLITE_DENY if the operation is denied
func secureAuthorizer(action int, arg1, arg2, dbName string) int {
	// When creating tables, SQLite needs to update sqlite_master
	if arg1 == "sqlite_master" || arg1 == "sqlite_schema" {
		return SQLITE_OK // Allow operations on internal system tables
	}

	// Allow only SELECT, INSERT, CREATE TABLE operations and necessary supporting actions
	switch action {
	case SQLITE_SELECT:
		// Allow all SELECT operations
		return SQLITE_OK
		
	case SQLITE_INSERT:
		// Allow all INSERT operations
		return SQLITE_OK
		
	case SQLITE_CREATE_TABLE:
		// Allow CREATE TABLE operations
		return SQLITE_OK
		
	case SQLITE_READ:
		// Allow read operations (necessary for SELECT)
		return SQLITE_OK
		
	case SQLITE_FUNCTION:
		// Allow SQLite functions like COUNT(), SUM(), etc.
		return SQLITE_OK
		
	case SQLITE_TRANSACTION:
		// Allow transaction operations (BEGIN, COMMIT, ROLLBACK)
		return SQLITE_OK
		
	case SQLITE_UPDATE:
		// Allow UPDATE operations on all tables
		return SQLITE_OK
		
	case SQLITE_PRAGMA:
		// Only allow specific safe PRAGMAs
		safePragmas := map[string]bool{
			"table_info":    true, // Needed for schema inspection
			"foreign_keys":  true, // For referential integrity
			"index_list":    true, // For schema information
			"index_info":    true, // For schema information
			"table_xinfo":   true, // Extended table info
			"index_xinfo":   true, // Extended index info
			"collation_list": true, // For collation information
		}
		
		if safePragmas[arg1] {
			return SQLITE_OK
		}
		log.Printf("SECURITY: Blocked PRAGMA: %s", arg1)
		return SQLITE_DENY
		
	default:
		// Log detailed information for debugging
		log.Printf("SECURITY: Blocked operation code: %d on %s.%s.%s", action, dbName, arg1, arg2)
		return SQLITE_DENY
	}
}

// GetSecureDBConnection opens a database connection with security restrictions
func GetSecureDBConnection(dbPath string) (*sql.DB, error) {
	// Use our secure driver with the authorizer
	db, err := sql.Open("sqlite3_secure", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open secure database: %w", err)
	}
	
	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to secure database: %w", err)
	}
	
	// Enable foreign keys for referential integrity
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to enable foreign keys: %w", err)
	}
	
	return db, nil
}
