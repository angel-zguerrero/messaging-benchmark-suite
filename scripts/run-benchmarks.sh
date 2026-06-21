#!/bin/bash

# Ensure script stops on error
set -e

# Default values
N=${1:-10}
M=${2:-10}
TARGET=${3:-"rabbitmq"}
QUORUM=${4:-"false"}

if [[ "$TARGET" != "rabbitmq" && "$TARGET" != "daedalus" ]]; then
    echo "Error: Target must be 'rabbitmq' or 'daedalus'"
    exit 1
fi

echo "============================================================"
echo "Starting Unified Node.js Benchmark for $TARGET"
echo "Publishers (N): $N"
echo "Consumers (M):  $M"
echo "Quorum Queue:   $QUORUM"
echo "============================================================"

# Ensure infrastructure is up
echo "Ensuring infrastructure is running..."
docker-compose up -d rabbitmq daedalus influxdb telegraf grafana

# Build the custom runner image
echo "Building Node.js benchmark runner..."
docker build -t benchmark-runner -f Dockerfile.runner .

# Run the benchmark script
echo "Running benchmark script (Press Ctrl+C to stop)..."
docker run --rm -it \
  --network benchmark_net \
  -e N=$N -e M=$M \
  -e TARGET=$TARGET \
  -e QUORUM=$QUORUM \
  benchmark-runner

echo "============================================================"
echo "Benchmark stopped. You can view results in Grafana at http://localhost:3001"
echo "============================================================"
