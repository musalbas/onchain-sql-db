package sql_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"database/sql"
	_ "github.com/mattn/go-sqlite3" // Register sqlite3 driver
	
	localsql "github.com/musalbas/onchain-sql-db/pkg/sql"
)

func TestSQLiteAuthorizer(t *testing.T) {
	// Create a test database file
	dbPath := "test_auth.db"
	
	// Clean up any existing test database
	os.Remove(dbPath)
	
	// Create a new SQL manager with our security restrictions
	manager, err := localsql.NewManager(dbPath)
	if err != nil {
		t.Fatalf("Failed to create SQL manager: %v", err)
	}
	defer manager.Close()
	
	// Create a standard database connection without restrictions for comparison
	stdDb, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Failed to create standard database connection: %v", err)
	}
	defer stdDb.Close()
	
	fmt.Println("Testing SQLite authorizer...")
	
	// Test cases - should pass
	allowedQueries := []string{
		// CREATE TABLE operation
		"CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)",
		
		// INSERT operation
		"INSERT INTO test_table (name) VALUES ('test data')",
		"INSERT INTO test_table (name) VALUES ('more data')",
		
		// UPDATE operation
		"UPDATE test_table SET name = 'updated' WHERE id = 1",
		
		// SELECT operation
		"SELECT * FROM test_table",
		"SELECT COUNT(*) FROM test_table",
	}
	
	// Test cases - should be blocked
	blockedQueries := []struct {
		query string
		setup string // Optional setup query to run first with standard DB connection
	}{
		// DROP operation
		{"DROP TABLE test_table", ""},
		
		// DELETE operation
		{"DELETE FROM test_table", ""},
		
		// ALTER TABLE operation
		{"ALTER TABLE test_table ADD COLUMN new_column TEXT", ""},
		
		// ATTACH DATABASE operation
		{"ATTACH DATABASE 'other.db' AS other", ""},
		
		// PRAGMA operation (not in whitelist)
		{"PRAGMA journal_mode = WAL", ""},
	}
	
	// Run allowed queries - should succeed
	fmt.Println("\n=== Testing allowed operations ===")
	for _, query := range allowedQueries {
		fmt.Printf("Running: %s\n", query)
		results, err := manager.ExecuteQuery(query)
		if err != nil {
			t.Errorf("[FAIL] Query should be allowed but was rejected: %s\nError: %v", query, err)
		} else {
			fmt.Printf("[PASS] Query executed\n")
			if len(results) > 0 {
				fmt.Printf("  Results: %v\n", results)
			}
		}
		fmt.Println()
	}
	
	// Run blocked queries - should fail with our secure manager but succeed with standard connection
	fmt.Println("\n=== Testing blocked operations ===")
	for _, testCase := range blockedQueries {
		// First, ensure the table exists for operations that need it
		if testCase.setup != "" {
			_, err := stdDb.Exec(testCase.setup)
			if err != nil {
				t.Fatalf("Setup failed: %v", err)
			}
		}
		
		fmt.Printf("Running: %s\n", testCase.query)
		
		// Try with secure manager - should be blocked
		_, secureErr := manager.ExecuteQuery(testCase.query)
		
		// Try with standard connection - should work if the query is valid
		stdErr := func() error {
			// Skip certain operations that might cause issues with standard connection
			if testCase.query == "ATTACH DATABASE 'other.db' AS other" {
				return nil // Pretend it succeeded
			}
			_, err := stdDb.Exec(testCase.query)
			return err
		}()
		
		// Check if the error is due to authorization
		if secureErr != nil {
			authError := secureErr.Error()
			if (stdErr == nil || stdErr.Error() != secureErr.Error()) && 
			   (strings.Contains(authError, "not authorized") || 
			    strings.Contains(authError, "security")) {
				// This is what we want - blocked by authorization, not some other reason
				fmt.Printf("[PASS] Correctly blocked by authorizer: %v\n", secureErr)
			} else {
				// Failed, but not because of authorizer
				t.Errorf("[UNEXPECTED] Query failed but not due to authorization: %v\nStd error: %v", secureErr, stdErr)
			}
		} else {
			// Query succeeded when it should have been blocked
			t.Errorf("[FAIL] SECURITY FAILURE: Query was allowed but should have been blocked: %s", testCase.query)
		}
		fmt.Println()
	}
	
	fmt.Println("Test completed!")
}
