import { InfluxDB, FieldType } from 'influx';
import { RabbitMQAdapter } from './adapters/RabbitMQAdapter';
import { DaedalusAdapter } from './adapters/DaedalusAdapter';
import { IMessagingAdapter } from './interfaces';

const PUBLISHERS = parseInt(process.env.N || '10', 10);
const NUM_QUEUES = parseInt(process.env.Q || '1', 10);
const WORKERS_PER_QUEUE = parseInt(process.env.W || '1', 10);
const TARGET = process.env.TARGET || 'rabbitmq';
const isQuorum = process.env.QUORUM === 'true';
const BASE_QUEUE_NAME = isQuorum ? 'benchmark_queue_quorum' : 'benchmark_queue';
const QUEUE_NAMES = Array.from({ length: NUM_QUEUES }, (_, i) => `${BASE_QUEUE_NAME}_${i + 1}`);

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
                consumed: FieldType.INTEGER
            },
            tags: ['broker', 'scenario']
        }
    ]
});

let publishedCount = 0;
let consumedCount = 0;
let isRunning = true;

async function runPublisher(adapter: IMessagingAdapter, id: number, queues: string[]) {
    let queueIdx = 0;
    while (isRunning) {
        try {
            const queue = queues[queueIdx];
            await adapter.publish(queue, {
                timestamp: Date.now(),
                publisherId: id,
                data: "Hello from unified benchmark suite!",
                padding: "x".repeat(500) // Simulate a moderately sized payload
            });
            queueIdx = (queueIdx + 1) % queues.length;
            publishedCount++;
            
            // Yield to the event loop so we don't completely lock Node
            await new Promise(r => setImmediate(r));
        } catch (err) {
            console.error("Publish error", err);
            await new Promise(r => setTimeout(r, 1000)); // backoff
        }
    }
}

async function startBenchmark() {
    console.log(`Starting benchmark for ${TARGET} with ${PUBLISHERS} publishers and ${NUM_QUEUES} queues.`);
    
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

    let adapter: IMessagingAdapter;
    if (TARGET === 'rabbitmq') {
        adapter = new RabbitMQAdapter();
    } else if (TARGET === 'daedalus') {
        adapter = new DaedalusAdapter();
    } else {
        throw new Error(`Unknown target: ${TARGET}`);
    }

    try {
        await adapter.connect();
        // Setup all queues in one call
        await adapter.setup(QUEUE_NAMES);
        console.log(`✅ ${TARGET} setup complete.`);
    } catch (err) {
        console.error(`❌ Failed to setup ${TARGET}:`, err);
        process.exit(1);
    }

    // Start consumers (workers)
    for (let w = 0; w < WORKERS_PER_QUEUE; w++) {
        for (const q of QUEUE_NAMES) {
            await adapter.consume(q, async (msg, ack) => {
                consumedCount++;
                await ack();
            });
        }
    }
    console.log(`🚀 ${(WORKERS_PER_QUEUE * NUM_QUEUES)} consumers started.`);

    // Start publishers
    for (let i = 0; i < PUBLISHERS; i++) {
        runPublisher(adapter, i, QUEUE_NAMES);
    }
    console.log(`🚀 ${PUBLISHERS} publishers started.`);

    // Metrics reporting loop
    setInterval(async () => {
        const currentPub = publishedCount;
        const currentCon = consumedCount;
        publishedCount = 0; // Reset for the next second
        consumedCount = 0;

        console.log(`[${TARGET}] Published: ${currentPub} msgs/sec | Consumed: ${currentCon} msgs/sec`);

        try {
            await influx.writePoints([
                {
                    measurement: 'throughput',
                    tags: { broker: TARGET, scenario: `${PUBLISHERS}p_${WORKERS_PER_QUEUE}w_${NUM_QUEUES}q` },
                    fields: { published: currentPub, consumed: currentCon },
                }
            ]);
        } catch (e) {
            console.error("Error writing to InfluxDB", e);
        }
    }, 1000);

    // Stop cleanly on interrupt
    process.on('SIGINT', async () => {
        console.log("Shutting down...");
        isRunning = false;
        await adapter.disconnect();
        process.exit(0);
    });
}

startBenchmark().catch(console.error);
