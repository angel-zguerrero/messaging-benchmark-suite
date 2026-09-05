#!/bin/bash

# Ensure script stops on error
set -e

# Default values
N=${1:-10}
Q=${2:-1}
W=${3:-1}
TARGET=${4:-"rabbitmq"}
QUORUM=${5:-"false"}
# ACTIVE_RATIO: fraction of declared queues that get live consumers (0.0 - 1.0).
# Set to less than 1.0 to test idle queue memory footprint (the "sparse consumer" scenario).
# Example: 0.2 means 20% of queues have consumers; the other 80% are declared but idle.
ACTIVE_RATIO=${6:-"1.0"}

if [[ "$TARGET" != "rabbitmq" && "$TARGET" != "daedalus" && "$TARGET" != "bullmq" ]]; then
    echo "Error: Target must be 'rabbitmq', 'daedalus', or 'bullmq'"
    exit 1
fi

echo "============================================================"
echo "Starting Unified Node.js Benchmark for $TARGET"
echo "Publishers (N):        $N"
echo "Queues (Q):            $Q"
echo "Workers per queue (W): $W"
echo "Quorum Queue:          $QUORUM"
echo "Active Ratio:          $ACTIVE_RATIO  ($(echo "$ACTIVE_RATIO * 100" | bc | cut -d. -f1)% of queues get consumers)"
echo "============================================================"

# Ensure infrastructure is up
echo "Ensuring infrastructure is running..."
docker-compose up -d rabbitmq redis influxdb telegraf grafana

# Always recreate Daedalus to wipe stale queue state from previous runs.
# We use -V to ensure anonymous volumes are recreated (wiping Raft state).
echo "Recreating Daedalus container (clears stale queue state)..."
docker-compose up -d --force-recreate -V daedalus

# Build the custom runner image
echo "Building Node.js benchmark runner..."
docker build -t benchmark-runner -f Dockerfile.runner .

# Run the benchmark script
echo "Running benchmark script (Press Ctrl+C to stop)..."
docker run --rm \
  --name benchmark-runner-job \
  --label com.docker.compose.project=messaging-benchmark-suite \
  --network benchmark_net \
  -e N=$N -e Q=$Q -e W=$W \
  -e TARGET=$TARGET \
  -e QUORUM=$QUORUM \
  -e ACTIVE_RATIO=$ACTIVE_RATIO \
  -e DAEDALUS_URL="${DAEDALUS_URL:-http://daedalus:4000}" \
  -e REDIS_URL="${REDIS_URL:-redis://redis:6379}" \
  benchmark-runner

echo "============================================================"
echo "Benchmark stopped. You can view results in Grafana at http://localhost:3001"
echo "============================================================"
