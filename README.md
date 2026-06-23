# Messaging Benchmark Suite

This repository contains a unified benchmarking suite designed to compare the throughput and hardware resource consumption of **RabbitMQ** against **Daedalus Orchestrator** under heavy load.

The suite uses a custom Node.js runner to publish and consume thousands of messages per second, while automatically tracking hardware metrics via Telegraf and displaying real-time results in a pre-configured Grafana dashboard.

## Architecture

*   **Node.js Runner**: A scalable runner that spins up configurable `N` publishers and `M` consumers. It supports interchangeable adapters for RabbitMQ (`rascal`) and Daedalus (`@omicron-x/daedalus-sdk`).
*   **Message Brokers**:
    *   **RabbitMQ**: Evaluated in both "Classic" (in-memory) and "Quorum" (disk-backed consensus via Raft) modes.
    *   **Daedalus Orchestrator**: A durable workflow and messaging orchestrator.
*   **Telemetry Stack**:
    *   **Telegraf**: Monitors Docker socket (`/var/run/docker.sock`) to extract precise CPU and RAM usage for the broker containers.
    *   **InfluxDB**: Time-series database storing both application throughput metrics (msgs/sec) and hardware metrics.
    *   **Grafana**: Pre-provisioned dashboards for instant visualization.

## Prerequisites

*   Docker
*   Docker Compose

## How to Run the Benchmarks

A unified shell script (`run-benchmarks.sh`) is provided to automate the setup of the infrastructure, the building of the Dockerized Node.js runner, and the execution of the load tests.

**Syntax:**
```bash
./scripts/run-benchmarks.sh <Publishers> <Consumers> <Broker> [QuorumMode]
```

### 1. RabbitMQ (Classic In-Memory Queues)
This is the default mode. It tests RabbitMQ using volatile, in-memory queues. It is extremely fast but does not guarantee durability on disk in case of a crash.

```bash
# Runs 10 publishers and 10 consumers against RabbitMQ
./scripts/run-benchmarks.sh 10 10 rabbitmq
```

### 2. RabbitMQ (Quorum Queues)
Quorum queues force RabbitMQ to replicate and persist messages to disk using the Raft consensus algorithm before acknowledging them. **This provides a much fairer "apples-to-apples" comparison against Daedalus**, which also writes state to disk by default.

```bash
# The 'true' parameter activates Quorum Queues
./scripts/run-benchmarks.sh 10 10 rabbitmq true
```

### 3. Daedalus Orchestrator
Tests the Daedalus SDK. Daedalus persists state and ensures reliable delivery guarantees.

```bash
# Runs 10 publishers and 10 consumers against Daedalus
./scripts/run-benchmarks.sh 10 10 daedalus
```

*(Note: During the first 5-10 seconds of running Daedalus, you may see `ECONNREFUSED` errors in the console while the gRPC server finishes booting up. The SDK will automatically reconnect and start the test).*

## Viewing the Results (Grafana)

The entire telemetry stack is pre-configured. You do not need to build queries manually.

1. Once the benchmark is running, open your browser and navigate to **[http://localhost:3001](http://localhost:3001)**.
2. No login is required (Anonymous access is enabled).
3. Go to the left menu -> **Dashboards** -> Select **Messaging Benchmark**.

### Metrics Displayed
*   **Throughput**: Real-time graph showing `Published` vs `Consumed` messages per second.
*   **CPU Usage (%)**: Real-time Docker CPU consumption of the active broker container.
*   **RAM Usage**: Real-time Docker Memory consumption of the active broker container.

## Shutting Down

To stop the benchmark, simply press `Ctrl+C` in your terminal. 

To completely destroy the infrastructure and wipe the database:
```bash
docker-compose down -v
```
