# Messaging Benchmark Suite

This repository contains a unified benchmark suite designed to compare the throughput and hardware resource consumption of **RabbitMQ** vs. **Daedalus Orchestrator** vs. **BullMQ** under heavy workloads.

The suite uses a custom Node.js runner to publish and consume thousands of messages per second, while automatically monitoring hardware metrics via Telegraf and displaying real-time results in a pre-configured Grafana dashboard.

## Architecture

*   **Node.js Runner**: A scalable runner that initializes a configurable number of publishers, queues, and workers. Supports pluggable adapters for RabbitMQ (`rascal`), Daedalus (`@omicron-x/daedalus-sdk`), and BullMQ (`bullmq`). The key architectural difference is how each adapter implements workers:
    *   **RabbitMQ**: W workers × Q queues = W×Q open AMQP channels (one dedicated channel per queue, per worker — even when the queue is empty).
    *   **Daedalus**: W polling workers total, each covering all Q queues. No channel is held open per queue; idle queues cost zero RAM or connections.
    *   **BullMQ**: W workers × Q queues = W×Q BullMQ workers connected to Redis.
*   **Message Brokers**:
    *   **RabbitMQ**: Evaluated in both "Classic" (in-memory) and "Quorum" (disk-backed consensus via Raft) modes.
    *   **Daedalus Orchestrator**: A durable workflow and messaging orchestrator.
    *   **BullMQ**: A Redis-based message and job queue for Node.js.
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
| `W` / `$3` | `W` | Number of concurrent worker units | `1` | Integer (e.g., `1`, `5`, `10`) |
| `TARGET` / `$4` | `TARGET` | Target messaging broker or comma-separated list of brokers | `rabbitmq` | `rabbitmq`, `daedalus`, `bullmq` (or e.g. `rabbitmq,daedalus,bullmq`) |
| `QUORUM` / `$5` | `QUORUM` | Enables Quorum queues (RabbitMQ only) | `false` | `true`, `false` |
| `ACTIVE_RATIO` / `$6` | `ACTIVE_RATIO` | Fraction of queues that publishers write to | `1.0` | Float `0.0`–`1.0` (e.g., `0.2`) |
| - | `RABBITMQ_URL` | AMQP connection URL for RabbitMQ | `amqp://guest:guest@rabbitmq:5672` | AMQP URI string |
| - | `DAEDALUS_URL` | gRPC/HTTP service URL for Daedalus | `http://daedalus:4000` | HTTP/gRPC URI string |
| - | `REDIS_URL` | Redis connection URL for BullMQ | `redis://redis:6379` | Redis URI string |

> [!NOTE]
> **`W` means different things to each broker — this is intentional:**
> - **RabbitMQ**: W consumers per queue → `W × Q` total open AMQP channels. Every queue holds
>   a dedicated channel even when it has zero messages.
> - **Daedalus**: W polling workers total → each worker covers ALL Q queues. Idle queues have
>   zero live connections; they stay on Pebble disk until a message arrives.
>
> `ACTIVE_RATIO` controls which queues **receive published messages**. ALL queues are monitored
> by consumers on both brokers regardless — only the message traffic is sparse.

## How to Run Benchmarks

A unified script (`run-benchmarks.sh`) is provided to automate infrastructure setup, Node.js Docker image builds, and load test execution.

**Syntax:**
```bash
./scripts/run-benchmarks.sh <N> <Q> <W> <TARGET> [QUORUM] [ACTIVE_RATIO]
```

### 1. RabbitMQ (Classic In-Memory Queues)
This is the default mode. Tests RabbitMQ using volatile in-memory queues. It is extremely fast but does not guarantee disk durability in case of a crash.

```bash
# 10 publishers, 10 queues, 1 worker → RabbitMQ opens 1×10 = 10 AMQP channels
./scripts/run-benchmarks.sh 10 10 1 rabbitmq

# 10 publishers, 10 queues, 2 workers → RabbitMQ opens 2×10 = 20 AMQP channels
./scripts/run-benchmarks.sh 10 10 2 rabbitmq
```

### 2. RabbitMQ (Quorum Queues)
Quorum queues force RabbitMQ to replicate and persist messages to disk using the Raft consensus algorithm before acknowledging. **This provides a fair apples-to-apples comparison against Daedalus**, which also writes state to disk by default.

```bash
# Enable Quorum Queues with 'true' as the 5th argument
./scripts/run-benchmarks.sh 10 10 1 rabbitmq true
```

### 3. BullMQ
Tests BullMQ using Redis. You can also view Redis keys and queues in real time via **Redis Commander** at **[http://localhost:8082](http://localhost:8082)**.

```bash
# 10 publishers, 10 queues, 1 worker → BullMQ creates 1×10 = 10 workers
./scripts/run-benchmarks.sh 10 10 1 bullmq
```

### 4. Daedalus Orchestrator
Tests the Daedalus SDK. Unlike RabbitMQ, `W` is the total number of polling workers — not per-queue.
With `W=2` and `Q=10`, Daedalus creates **2 workers** (not 20), each polling all 10 queues.

```bash
# 10 publishers, 10 queues, 2 workers → Daedalus creates exactly 2 polling workers (not 20)
./scripts/run-benchmarks.sh 10 10 2 daedalus
```

*(Note: During the first 5-10 seconds when executing Daedalus, reconnect warnings may appear in the console while the gRPC server finishes starting up. The SDK will automatically reconnect and start benchmarking).*

### 5. Running Multiple Brokers in Parallel (Comma-Separated)
You can specify a list of brokers separated by commas (e.g., `rabbitmq,daedalus,bullmq` or `rabbitmq,bullmq`) to run benchmark tests across multiple brokers in parallel indefinitely until `Ctrl+C`.

```bash
# Run all three brokers concurrently in parallel
./scripts/run-benchmarks.sh 10 10 1 rabbitmq,daedalus,bullmq

# Run RabbitMQ and BullMQ in parallel
./scripts/run-benchmarks.sh 10 10 1 rabbitmq,bullmq
```

### 6. Sparse Consumer — Memory Footprint Test (the key Daedalus advantage)

This scenario replicates the real-world 80/20 pattern: **80% of the time, only 20% of queues
receive tasks**. Both brokers must watch all Q queues because you never know which one will
receive the next task. The cost of watching idle queues is what differs:

| | RabbitMQ | Daedalus |
| :--- | :--- | :--- |
| **Q=1000 queues, W=2 workers** | Opens **2 × 1000 = 2000 AMQP channels** — one per queue per worker, always open | Creates **2 polling workers** that cover all 1000 queues — no per-queue channel |
| **Idle queue cost** | Channel + socket + RAM per queue | Zero — idle queue lives on Pebble disk, no connection held |
| **ACTIVE_RATIO=0.2** | Publishers write to 200/1000 queues; 800 queues have open channels with no messages | Publishers write to 200/1000 queues; 800 queues cost nothing while empty |
| **RAM at rest (idle queues)** | Scales with **total declared** queues | Scales with **active workers** (W), not queue count |

```bash
# Both commands: 1000 queues declared, publishers only write to 20% of them.
# ALL queues are monitored on both brokers.
# Watch the RAM panel in Grafana — the idle-queue cost shows up clearly.

# RabbitMQ (Quorum for a fair disk-persistence comparison)
# → Opens 2×1000 = 2000 AMQP channels
./scripts/run-benchmarks.sh 5 1000 2 rabbitmq true 0.2

# Daedalus
# → Creates exactly 2 polling workers covering all 1000 queues
./scripts/run-benchmarks.sh 5 1000 2 daedalus false 0.2
```

> [!IMPORTANT]
> Focus on **RAM Usage** in Grafana, not throughput.
> RabbitMQ must hold an AMQP channel open for every queue even when empty.
> Daedalus workers are not bound to individual queues — idle queues cost zero RAM.

## Viewing Results (Grafana)

The entire telemetry stack is pre-configured. No manual queries are required.

1. Once the benchmark is running, open your browser and go to **[http://localhost:3001](http://localhost:3001)**.
2. No login required (anonymous access is enabled).
3. Go to the left menu -> **Dashboards** -> Select **Messaging Benchmark**.

### Displayed Metrics
*   **Throughput**: Real-time graph showing `Published` vs `Consumed` messages per second.
*   **Total Tasks**: Real-time graph showing total enqueued (`Published`) vs dequeued (`Consumed`) tasks.
*   **CPU Usage (%)**: Real-time Docker CPU consumption of the active broker container.
*   **RAM Usage**: Real-time Docker Memory consumption of the active broker container.

## Stopping Benchmarks

To stop the benchmark, simply press `Ctrl+C` in your terminal. 

To completely destroy the infrastructure and wipe the database:
```bash
# Stop containers and remove volumes/database state
docker-compose down -v
```
