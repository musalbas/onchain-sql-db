#!/bin/bash

# Generate a random table name with timestamp and random string
RANDOM_SUFFIX=$(date +%Y%m%d%H%M%S)_$(cat /dev/urandom | tr -dc 'a-z0-9' | fold -w 8 | head -n 1)

# Default configuration
SERVER_URL="http://localhost:8080"
RECORDS=1000
CONCURRENCY=10
BATCH_SIZE=20
POLL_CONCURRENCY=20
TIMEOUT="10m"
POLL_INTERVAL="200ms"
TEST_TABLE="perftest_${RANDOM_SUFFIX}"
VERBOSE=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --server)
      SERVER_URL="$2"
      shift 2
      ;;
    --records)
      RECORDS="$2"
      shift 2
      ;;
    --concurrency)
      CONCURRENCY="$2"
      shift 2
      ;;
    --batchsize)
      BATCH_SIZE="$2"
      shift 2
      ;;
    --pollconcurrency)
      POLL_CONCURRENCY="$2"
      shift 2
      ;;
    --timeout)
      TIMEOUT="$2"
      shift 2
      ;;
    --pollinterval)
      POLL_INTERVAL="$2"
      shift 2
      ;;
    --table)
      TEST_TABLE="$2"
      shift 2
      ;;
    --verbose)
      VERBOSE=true
      shift
      ;;
    --help)
      echo "Usage: $0 [options]"
      echo "  --server URL           Server URL (default: http://localhost:8080)"
      echo "  --records N            Number of records to insert (default: 1000)"
      echo "  --concurrency N        Submission concurrency level (default: 10)"
      echo "  --batchsize N          Number of inserts per batch (default: 20)"
      echo "  --pollconcurrency N    Polling concurrency level (default: 20)"
      echo "  --timeout DURATION     Maximum time to wait for completion (default: 10m)"
      echo "  --pollinterval DURATION How often to check for committed transactions (default: 200ms)"
      echo "  --table NAME           Name of test table to create (default: perftest)"
      echo "  --verbose              Enable verbose logging"
      echo "  --help                 Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Run '$0 --help' for usage"
      exit 1
      ;;
  esac
done

# Print configuration
echo "Performance Test Configuration:"
echo "- Server URL: $SERVER_URL"
echo "- Records: $RECORDS"
echo "- Submission Concurrency: $CONCURRENCY"
echo "- Poll Concurrency: $POLL_CONCURRENCY"
echo "- Batch Size: $BATCH_SIZE"
echo "- Timeout: $TIMEOUT"
echo "- Poll Interval: $POLL_INTERVAL"
echo "- Test Table: $TEST_TABLE"
echo "- Verbose: $VERBOSE"
echo ""

# Build the performance test tool if it doesn't exist
if [ ! -f "./perftest" ]; then
  echo "Building performance test tool..."
  cd cmd/perftest && go build -o ../../perftest
  cd ../..
fi

# Run the performance test
VERBOSE_FLAG=""
if [ "$VERBOSE" = true ]; then
  VERBOSE_FLAG="--verbose"
fi

./perftest --server "$SERVER_URL" \
  --records "$RECORDS" \
  --concurrency "$CONCURRENCY" \
  --batchsize "$BATCH_SIZE" \
  --pollconcurrency "$POLL_CONCURRENCY" \
  --timeout "$TIMEOUT" \
  --pollinterval "$POLL_INTERVAL" \
  --table "$TEST_TABLE" \
  $VERBOSE_FLAG
