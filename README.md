# Messaging Benchmark Suite

This repository contains a unified benchmark suite designed to compare the throughput and hardware resource consumption of **RabbitMQ** vs. **Daedalus Orchestrator** under heavy workloads.

The suite uses a custom Node.js runner to publish and consume thousands of messages per second, while automatically monitoring hardware metrics via Telegraf and displaying real-time results in a pre-configured Grafana dashboard.

## Architecture

*   **Node.js Runner**: A scalable runner that initializes a configurable number of publishers, queues, and workers per queue. Supports pluggable adapters for RabbitMQ (`rascal`) and Daedalus (`@omicron-x/daedalus-sdk`).
*   **Message Brokers**:
    *   **RabbitMQ**: Evaluated in both "Classic" (in-memory) and "Quorum" (disk-backed consensus via Raft) modes.
    *   **Daedalus Orchestrator**: A durable workflow and messaging orchestrator.
*   **Telemetry Stack**:
    *   **Telegraf**: Monitors the Docker socket (`/var/run/docker.sock`) to extract precise CPU and RAM usage from broker containers.
    *   **InfluxDB**: Time-series database storing application throughput metrics (msgs/sec) as well as hardware metrics.
    *   **Grafana**: Pre-configured dashboards for instant visualization.

## Prerequisites

*   Docker
*   Docker Compose

## Configuration Parameters

The benchmark accepts the following parameters, configurable via positional CLI arguments in `run-benchmarks.sh` or environment variables when running the suite:

| Parameter | Environment Variable | Description | Default | Accepted Values |
| :--- | :--- | :--- | :--- | :--- |
| `N` / `$1` | `N` | Number of concurrent publisher loops | `10` | Integer (e.g., `0`, `10`, `100`) |
| `Q` / `$2` | `Q` | Number of created queues (`benchmark_queue_1..N`) | `1` | Integer (e.g., `1`, `10`, `200`) |
| `W` / `$3` | `W` | Number of workers (consumers) per queue | `1` | Integer (e.g., `1`, `5`) |
| `TARGET` / `$4` | `TARGET` | Target messaging broker | `rabbitmq` | `rabbitmq`, `daedalus` |
| `QUORUM` / `$5` | `QUORUM` | Enables Quorum queues (RabbitMQ only) | `false` | `true`, `false` |
| - | `RABBITMQ_URL` | AMQP connection URL for RabbitMQ | `amqp://guest:guest@rabbitmq:5672` | AMQP URI string |
| - | `DAEDALUS_URL` | gRPC/HTTP service URL for Daedalus | `http://daedalus:4000` | HTTP/gRPC URI string |

> [!NOTE]
> The total number of consumers created in the benchmark equals **`W * Q`** (Workers per queue × Number of queues).

## How to Run Benchmarks

A unified script (`run-benchmarks.sh`) is provided to automate infrastructure setup, Node.js Docker image builds, and load test execution.

**Syntax:**
```bash
./scripts/run-benchmarks.sh <N> <Q> <W> <TARGET> [QUORUM]
```

### 1. RabbitMQ (Classic In-Memory Queues)
This is the default mode. Tests RabbitMQ using volatile in-memory queues. It is extremely fast but does not guarantee disk durability in case of a crash.

```bash
# Run 10 publishers, 1 queue, and 1 worker per queue against RabbitMQ
./scripts/run-benchmarks.sh 10 1 1 rabbitmq

# Run 10 publishers, 10 queues, and 2 workers per queue (20 consumers total) against RabbitMQ
./scripts/run-benchmarks.sh 10 10 2 rabbitmq
```

### 2. RabbitMQ (Quorum Queues)
Quorum queues force RabbitMQ to replicate and persist messages to disk using the Raft consensus algorithm before acknowledging. **This provides a fair apples-to-apples comparison against Daedalus**, which also writes state to disk by default.

```bash
# Enable Quorum Queues with 'true' as the 5th argument
./scripts/run-benchmarks.sh 10 10 1 rabbitmq true
```

### 3. Daedalus Orchestrator
Tests the Daedalus SDK. Daedalus persists state and ensures reliable delivery guarantees.

```bash
# Run 10 publishers, 10 queues, and 1 worker per queue against Daedalus
./scripts/run-benchmarks.sh 10 10 1 daedalus

# Run consumer-only test (0 publishers, 200 queues, 5 workers per queue = 1000 consumers total)
./scripts/run-benchmarks.sh 0 200 5 daedalus
```

*(Note: During the first 5-10 seconds when executing Daedalus, reconnect warnings may appear in the console while the gRPC server finishes starting up. The SDK will automatically reconnect and start benchmarking).*

## Viewing Results (Grafana)

The entire telemetry stack is pre-configured. No manual queries are required.

1. Once the benchmark is running, open your browser and go to **[http://localhost:3001](http://localhost:3001)**.
2. No login required (anonymous access is enabled).
3. Go to the left menu -> **Dashboards** -> Select **Messaging Benchmark**.

### Displayed Metrics
*   **Throughput**: Real-time graph showing `Published` vs `Consumed` messages per second.
*   **CPU Usage (%)**: Real-time Docker CPU consumption of the active broker container.
*   **RAM Usage**: Real-time Docker Memory consumption of the active broker container.

## Stopping Benchmarks

To stop the benchmark, simply press `Ctrl+C` in your terminal. 

To completely destroy the infrastructure and wipe the database:
```bash
# Stop containers and remove volumes/database state
docker-compose down -v
```
