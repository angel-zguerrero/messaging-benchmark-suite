import { InfluxDB, FieldType } from 'influx';
import { RabbitMQAdapter } from './adapters/RabbitMQAdapter';
import { DaedalusAdapter } from './adapters/DaedalusAdapter';
import { IMessagingAdapter } from './interfaces';

const PUBLISHERS = parseInt(process.env.N || '10', 10);
const CONSUMERS = parseInt(process.env.M || '10', 10);
const TARGET = process.env.TARGET || 'rabbitmq';
const isQuorum = process.env.QUORUM === 'true';
const QUEUE_NAME = isQuorum ? 'benchmark_queue_quorum' : 'benchmark_queue';

const influx = new InfluxDB({
    host: 'influxdb',
    port: 8086,
    database: 'k6', // Reusing the same database name to avoid extra setup
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

async function runPublisher(adapter: IMessagingAdapter, id: number) {
    while (isRunning) {
        try {
            await adapter.publish(QUEUE_NAME, {
                timestamp: Date.now(),
                publisherId: id,
                data: "Hello from unified benchmark suite!",
                padding: "x".repeat(500) // Simulate a moderately sized payload
            });
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
    console.log(`Starting benchmark for ${TARGET} with ${PUBLISHERS} publishers and ${CONSUMERS} consumers.`);
    
    // Ensure InfluxDB database exists
    try {
        const names = await influx.getDatabaseNames();
        if (!names.includes('k6')) {
            await influx.createDatabase('k6');
            console.log("Created InfluxDB database 'k6'.");
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
        await adapter.setup(QUEUE_NAME);
        console.log(`✅ ${TARGET} setup complete.`);
    } catch (err) {
        console.error(`❌ Failed to setup ${TARGET}:`, err);
        process.exit(1);
    }

    // Start consumers
    for (let i = 0; i < CONSUMERS; i++) {
        await adapter.consume(QUEUE_NAME, async (msg, ack) => {
            consumedCount++;
            await ack();
        });
    }
    console.log(`🚀 ${CONSUMERS} consumers started.`);

    // Start publishers
    for (let i = 0; i < PUBLISHERS; i++) {
        runPublisher(adapter, i);
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
                    tags: { broker: TARGET, scenario: `${PUBLISHERS}p_${CONSUMERS}c` },
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
