# On-Chain SQL Database with Celestia

This project implements an on-chain SQL database using Celestia for data availability. It allows you to store SQL queries on the Celestia blockchain and replicate the database state by replaying those queries.

## Features

- HTTP server with PUT/GET endpoints for SQL queries
- Stores SQL queries on the Celestia blockchain
- SQLite database backend
- Replay functionality to rebuild database state from on-chain queries
- Configurable Celestia namespace

## Requirements

- Go 1.20+
- SQLite
- A running Celestia node (light node is sufficient)

## Installation

```bash
# Clone the repository
git clone https://github.com/mus/onchain-sql-db.git
cd onchain-sql-db

# Build the project
go build -o onchain-sql-db
```

## Usage

```bash
# Start the server with default settings
./onchain-sql-db

# Start with custom settings
./onchain-sql-db --port 8081 --db ./custom.db --celestia-url http://localhost:26658 --celestia-token your_auth_token --namespace DEADBEEF
```

### Command-line Options

- `--port`: HTTP server port (default: 8080)
- `--db`: Path to SQLite database (default: ./onchain.db)
- `--celestia-url`: Celestia node URL (default: http://localhost:26658)
- `--celestia-token`: Celestia node auth token (default: empty)
- `--namespace`: Celestia namespace in hex without 0x prefix (default: DEADBEEF)

## API Endpoints

### PUT /query

Executes an SQL query and stores it on the Celestia blockchain.

**Request Body:**
```json
{
  "query": "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)"
}
```

**Response:**
```json
{
  "success": true,
  "results": [[1]],
  "height": 123456
}
```

### GET /query?query=SELECT+*+FROM+users

Executes an SQL query locally without storing it on the blockchain.

**Response:**
```json
{
  "success": true,
  "results": [
    ["id", "name", "email"],
    [1, "John Doe", "john@example.com"]
  ]
}
```

### POST /replay

Replays queries from the Celestia blockchain to rebuild the database state.

**Request Body:**
```json
{
  "from_height": 100000,
  "to_height": 200000
}
```

**Response:**
```json
{
  "100001": {"success": true},
  "100254": {"success": true},
  "100872": {"success": false, "error": "syntax error"}
}
```

### GET /status

Returns the current status of the node.

**Response:**
```json
{
  "current_height": 123456,
  "processed_queries": 42,
  "celestia_namespace": "DEADBEEF",
  "celestia_connection": true
}
```

## Architecture

The project consists of three main components:

1. **HTTP Server**: Handles API requests and coordinates between the SQL and Celestia managers.
2. **SQL Manager**: Manages the SQLite database and executes SQL queries.
3. **Celestia Manager**: Interfaces with the Celestia blockchain to store and retrieve queries.

## Security Notice

⚠️ **IMPORTANT**: This code has not undergone formal security auditing and is intended as a non-production ready proof-of-concept. It should not be used in production environments or for storing sensitive data. The security mechanisms, while implemented with best intentions, have not been verified by security professionals.

## License

MIT
