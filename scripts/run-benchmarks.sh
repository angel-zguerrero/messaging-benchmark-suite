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
DURATION=${DURATION:-0}

# Parse comma-separated targets
IFS=',' read -ra RAW_TARGETS <<< "$TARGET"
CLEAN_TARGETS=()
for t in "${RAW_TARGETS[@]}"; do
    # Trim whitespace
    t_trimmed=$(echo "$t" | xargs)
    if [[ -n "$t_trimmed" ]]; then
        CLEAN_TARGETS+=("$t_trimmed")
    fi
done

if [[ ${#CLEAN_TARGETS[@]} -eq 0 ]]; then
    echo "Error: No target specified."
    exit 1
fi

for t in "${CLEAN_TARGETS[@]}"; do
    if [[ "$t" != "rabbitmq" && "$t" != "daedalus" && "$t" != "bullmq" ]]; then
        echo "Error: Target '$t' is invalid. Allowed targets are 'rabbitmq', 'daedalus', or 'bullmq'."
        exit 1
    fi
done

# If multiple targets are specified and DURATION is 0/not set, default DURATION to 30s per target
if [[ ${#CLEAN_TARGETS[@]} -gt 1 && "$DURATION" -eq 0 ]]; then
    DURATION=30
fi

echo "============================================================"
echo "Starting Unified Node.js Benchmark for target(s): ${CLEAN_TARGETS[*]}"
echo "Publishers (N):        $N"
echo "Queues (Q):            $Q"
echo "Workers per queue (W): $W"
echo "Quorum Queue:          $QUORUM"
echo "Active Ratio:          $ACTIVE_RATIO  ($(echo "$ACTIVE_RATIO * 100" | bc | cut -d. -f1)% of queues get consumers)"
if [[ "$DURATION" -gt 0 ]]; then
    echo "Duration per target:   ${DURATION}s"
else
    echo "Duration per target:   Indefinite (Press Ctrl+C to stop)"
fi
echo "============================================================"

# Ensure infrastructure is up
echo "Ensuring infrastructure is running..."
docker-compose up -d rabbitmq redis redis-commander influxdb telegraf grafana

# Build the custom runner image
echo "Building Node.js benchmark runner..."
docker build -t benchmark-runner -f Dockerfile.runner .

for TARGET_ITEM in "${CLEAN_TARGETS[@]}"; do
    echo "------------------------------------------------------------"
    echo "Running benchmark target: $TARGET_ITEM"
    echo "------------------------------------------------------------"

    if [[ "$TARGET_ITEM" == "daedalus" ]]; then
        # Always recreate Daedalus to wipe stale queue state from previous runs.
        # We use -V to ensure anonymous volumes are recreated (wiping Raft state).
        echo "Recreating Daedalus container (clears stale queue state)..."
        docker-compose up -d --force-recreate -V daedalus
    fi

    # Run the benchmark script
    echo "Running benchmark script for $TARGET_ITEM (Press Ctrl+C to stop)..."
    docker run --rm \
      --name benchmark-runner-job \
      --label com.docker.compose.project=messaging-benchmark-suite \
      --network benchmark_net \
      -e N=$N -e Q=$Q -e W=$W \
      -e TARGET=$TARGET_ITEM \
      -e QUORUM=$QUORUM \
      -e ACTIVE_RATIO=$ACTIVE_RATIO \
      -e DURATION=$DURATION \
      -e DAEDALUS_URL="${DAEDALUS_URL:-http://daedalus:4000}" \
      -e REDIS_URL="${REDIS_URL:-redis://redis:6379}" \
      benchmark-runner
done

echo "============================================================"
echo "Benchmark completed. You can view results in Grafana at http://localhost:3001"
echo "============================================================"
