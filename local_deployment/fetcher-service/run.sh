#!/bin/bash

# Chronon Fetcher Service - Run Script

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Configuration
export HOST="${HOST:-0.0.0.0}"
export PORT="${PORT:-8080}"

echo "=========================================="
echo "Chronon Fetcher Service"
echo "=========================================="
echo "Host: $HOST"
echo "Port: $PORT"
echo "=========================================="
echo ""
echo "Starting service..."
echo "Press Ctrl+C to stop"
echo ""

# Run the service (this will compile if needed)
sbt run

