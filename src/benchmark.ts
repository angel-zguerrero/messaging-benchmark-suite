import { InfluxDB, FieldType } from 'influx';
import { RabbitMQAdapter } from './adapters/RabbitMQAdapter';
import { DaedalusAdapter } from './adapters/DaedalusAdapter';
import { BullMQAdapter } from './adapters/BullMQAdapter';
import { IMessagingAdapter } from './interfaces';

const PUBLISHERS = parseInt(process.env.N || '10', 10);
const NUM_QUEUES  = parseInt(process.env.Q || '1', 10);
// W = number of concurrent worker units (machines/processes).
// Semantics differ per broker — this is the architectural difference being benchmarked:
//   RabbitMQ : W consumers per queue  →  W × Q open AMQP channels total
//   Daedalus : W workers total        →  each polls ALL Q queues (no per-queue channel)
const WORKERS = parseInt(process.env.W || '1', 10);
const TARGET_ENV = process.env.TARGET || 'rabbitmq';
const isQuorum = process.env.QUORUM === 'true';

// Parse targets from comma-separated string
const TARGETS = TARGET_ENV.split(',').map(t => t.trim().toLowerCase()).filter(t => t.length > 0);
const VALID_TARGETS = ['rabbitmq', 'daedalus', 'bullmq'];

for (const t of TARGETS) {
    if (!VALID_TARGETS.includes(t)) {
        throw new Error(`Invalid target '${t}'. Supported targets are: ${VALID_TARGETS.join(', ')}`);
    }
}

// ACTIVE_RATIO controls what fraction of queues publishers push messages into (0.0 – 1.0).
// ALL queues are declared and monitored by consumers on both brokers regardless of this value.
// Setting this to 0.2 simulates the real-world 80/20 pattern:
//   "80% of the time, only 20% of queues have tasks arriving."
// The key observation: both systems must watch all Q queues, but the cost of doing so differs.
const ACTIVE_RATIO  = Math.min(1.0, Math.max(0.0, parseFloat(process.env.ACTIVE_RATIO || '1.0')));
const ACTIVE_Q_COUNT = Math.max(1, Math.ceil(NUM_QUEUES * ACTIVE_RATIO));

const BASE_QUEUE_NAME = isQuorum ? 'benchmark_queue_quorum' : 'benchmark_queue';

// All queues: declared on the broker and monitored by consumers.
const QUEUE_NAMES = Array.from({ length: NUM_QUEUES }, (_, i) => `${BASE_QUEUE_NAME}_${i + 1}`);

// Active queues: only these receive published messages. The rest are watched but stay empty.
// This simulates the common scenario where most queues are idle at any given moment.
const ACTIVE_QUEUE_NAMES = QUEUE_NAMES.slice(0, ACTIVE_Q_COUNT);

const INFLUX_DB = 'benchmark_metrics';

const influx = new InfluxDB({
    host: 'influxdb',
    port: 8086,
    database: INFLUX_DB,
    schema: [
        {
            measurement: 'throughput',
            fields: {
                published: FieldType.INTEGER,
                consumed: FieldType.INTEGER,
                total_published: FieldType.INTEGER,
                total_consumed: FieldType.INTEGER
            },
            tags: ['broker', 'scenario']
        }
    ]
});

// Scenario tag stored in InfluxDB — includes active ratio for easy Grafana filtering
const activePct   = Math.round(ACTIVE_RATIO * 100);
const SCENARIO_TAG = `${PUBLISHERS}p_${WORKERS}w_${NUM_QUEUES}q_${activePct}pct_active`;

const PUBLISH_BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '50', 10);

async function runPublisher(
    adapter: IMessagingAdapter,
    id: number,
    queues: string[],
    getIsRunning: () => boolean,
    onBatchPublished: (count: number) => void
) {
    let queueIdx = 0;
    while (getIsRunning()) {
        try {
            const promises = [];
            for (let b = 0; b < PUBLISH_BATCH_SIZE; b++) {
                const queue = queues[queueIdx];
                promises.push(adapter.publish(queue, {
                    timestamp:   Date.now(),
                    publisherId: id,
                    data:        "Hello from unified benchmark suite!",
                    padding:     "x".repeat(500) // simulate a moderately sized payload
                }));
                queueIdx = (queueIdx + 1) % queues.length;
            }
            
            // Wait for the entire batch to confirm.
            await Promise.all(promises);
            onBatchPublished(PUBLISH_BATCH_SIZE);

            // Yield to the event loop so we don't completely starve other tasks
            await new Promise(r => setImmediate(r));
        } catch (err) {
            if (getIsRunning()) {
                console.error("Publish error", err);
                await new Promise(r => setTimeout(r, 1000)); // back off on error
            }
        }
    }
}

const activeCleanupFns: Array<() => Promise<void>> = [];

async function startSingleTargetBenchmark(target: string): Promise<() => Promise<void>> {
    let publishedCount = 0;
    let consumedCount  = 0;
    let totalPublishedCount = 0;
    let totalConsumedCount  = 0;
    let isRunning      = true;

    console.log('='.repeat(60));
    console.log(`Starting benchmark target : ${target}`);
    console.log(`Total queues              : ${NUM_QUEUES}  (all declared + monitored)`);
    console.log(`Active queues             : ${ACTIVE_Q_COUNT}  (${activePct}% — publishers send here)`);
    console.log(`Idle queues               : ${NUM_QUEUES - ACTIVE_Q_COUNT}  (watched, zero messages)`);
    console.log(`Workers (W)               : ${WORKERS}`);
    if (target === 'rabbitmq') {
        console.log(`  → RabbitMQ model: ${WORKERS} consumers/queue × ${NUM_QUEUES} queues = ${WORKERS * NUM_QUEUES} open AMQP channels`);
    } else if (target === 'bullmq') {
        console.log(`  → BullMQ model: ${WORKERS} workers/queue × ${NUM_QUEUES} queues = ${WORKERS * NUM_QUEUES} BullMQ workers`);
    } else {
        console.log(`  → Daedalus model: ${WORKERS} workers total (each polls all ${NUM_QUEUES} queues)`);
    }
    console.log(`Publishers (N)            : ${PUBLISHERS}`);
    console.log(`Scenario tag              : ${SCENARIO_TAG}`);
    console.log('='.repeat(60));

    let adapter: IMessagingAdapter;
    if (target === 'rabbitmq') {
        adapter = new RabbitMQAdapter();
    } else if (target === 'daedalus') {
        adapter = new DaedalusAdapter();
    } else if (target === 'bullmq') {
        adapter = new BullMQAdapter();
    } else {
        throw new Error(`Unknown target: ${target}`);
    }

    try {
        await adapter.connect();
        // Declare ALL queues on the broker so both pay the same "declared queue" cost.
        await adapter.setup(QUEUE_NAMES);
        console.log(`✅ ${target} setup complete — ${NUM_QUEUES} queues declared.`);
    } catch (err) {
        console.error(`❌ Failed to setup ${target}:`, err);
        throw err;
    }

    // Start consumers on ALL queues.
    const { totalConsumers, description } = await adapter.startConsumers(
        QUEUE_NAMES,
        WORKERS,
        async (msg, ack) => {
            consumedCount++;
            totalConsumedCount++;
            await ack();
        }
    );
    console.log(`🚀 Consumers started for ${target}: ${description}`);

    // Publishers only send to ACTIVE queues.
    for (let i = 0; i < PUBLISHERS; i++) {
        runPublisher(
            adapter,
            i,
            ACTIVE_QUEUE_NAMES,
            () => isRunning,
            (batchSize) => {
                publishedCount += batchSize;
                totalPublishedCount += batchSize;
            }
        );
    }
    console.log(`🚀 ${PUBLISHERS} publishers started for ${target} → writing to ${ACTIVE_Q_COUNT}/${NUM_QUEUES} queues (${activePct}% active).`);

    // Metrics reporting loop — writes throughput to InfluxDB every second
    const intervalId = setInterval(async () => {
        const currentPub = publishedCount;
        const currentCon = consumedCount;
        publishedCount = 0;
        consumedCount  = 0;

        console.log(`[${target}] Published: ${currentPub} msgs/sec | Consumed: ${currentCon} msgs/sec | Total Published: ${totalPublishedCount} | Total Consumed: ${totalConsumedCount} | Workers: ${totalConsumers} | Queues: ${ACTIVE_Q_COUNT} active / ${NUM_QUEUES} total`);

        try {
            await influx.writePoints([
                {
                    measurement: 'throughput',
                    tags:   { broker: target, scenario: SCENARIO_TAG },
                    fields: {
                        published: currentPub,
                        consumed: currentCon,
                        total_published: totalPublishedCount,
                        total_consumed: totalConsumedCount
                    },
                }
            ]);
        } catch (e) {
            console.error("Error writing to InfluxDB", e);
        }
    }, 1000);

    return async () => {
        isRunning = false;
        clearInterval(intervalId);
        try {
            await adapter.disconnect();
        } catch (e) {
            console.error(`Error disconnecting adapter for ${target}:`, e);
        }
    };
}

async function startBenchmark() {
    // Ensure InfluxDB database exists
    try {
        const names = await influx.getDatabaseNames();
        if (!names.includes(INFLUX_DB)) {
            await influx.createDatabase(INFLUX_DB);
            console.log(`Created InfluxDB database '${INFLUX_DB}'.`);
        }
    } catch (e) {
        console.error("Warning: Could not connect to InfluxDB, metrics won't be saved.", e);
    }

    console.log(`Running benchmark targets in parallel: ${TARGETS.join(', ')}`);

    for (const target of TARGETS) {
        const cleanup = await startSingleTargetBenchmark(target);
        activeCleanupFns.push(cleanup);
    }

    console.log(`🚀 All target benchmarks (${TARGETS.join(', ')}) running in parallel (Press Ctrl+C to stop).`);
}

process.on('SIGINT', async () => {
    console.log("\nShutting down all benchmark targets...");
    for (const cleanup of activeCleanupFns) {
        await cleanup();
    }
    process.exit(0);
});

startBenchmark().catch(console.error);
