import amqplib from 'amqplib';
import { IMessagingAdapter } from '../interfaces';

export class RabbitMQAdapter implements IMessagingAdapter {
    private setupConnection: any = null;
    private publisherConnections: any[] = [];
    private publisherChannels: any[] = [];
    private nextPublisherIdx = 0;
    private consumerConnections: any[] = [];

    async connect(): Promise<void> {
        // Connection management is done inside setup() and startConsumers()
    }

    async setup(queueNames: string[]): Promise<void> {
        const baseUrl = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672';
        // Append heartbeat parameter to URL options
        const url = baseUrl.includes('?') ? `${baseUrl}&heartbeat=0` : `${baseUrl}?heartbeat=0`;

        // Connect with heartbeat disabled (0) via URL option so massive batch topology assertions do not trigger heartbeat timeouts
        this.setupConnection = await amqplib.connect(url);
        const channel = await this.setupConnection.createChannel();

        const exchangeName = 'benchmark_ex';
        await channel.assertExchange(exchangeName, 'topic', { durable: true });

        const isQuorum = process.env.QUORUM === 'true';
        const queueOptions = isQuorum
            ? { durable: true, arguments: { 'x-queue-type': 'quorum' } }
            : { durable: true };

        const BATCH_SIZE = 500;
        console.log(`Setting up ${queueNames.length} queues in batches of ${BATCH_SIZE}...`);

        for (let i = 0; i < queueNames.length; i += BATCH_SIZE) {
            const batch = queueNames.slice(i, i + BATCH_SIZE);
            await Promise.all(
                batch.map(async (queueName) => {
                    await channel.assertQueue(queueName, queueOptions);
                    await channel.bindQueue(queueName, exchangeName, queueName);
                })
            );
            if ((i + BATCH_SIZE) % 5000 === 0 || i + BATCH_SIZE >= queueNames.length) {
                console.log(`  Processed ${Math.min(i + BATCH_SIZE, queueNames.length)} / ${queueNames.length} queues...`);
            }
        }

        await channel.close();

        // Initialize publisher confirm channels
        const NUM_PUB_CONNECTIONS = 4;
        this.publisherConnections = [];
        this.publisherChannels = [];
        const pubUrl = baseUrl.includes('?') ? `${baseUrl}&heartbeat=60` : `${baseUrl}?heartbeat=60`;
        for (let i = 0; i < NUM_PUB_CONNECTIONS; i++) {
            const conn = await amqplib.connect(pubUrl);
            const pubChan = await conn.createConfirmChannel();
            this.publisherConnections.push(conn);
            this.publisherChannels.push(pubChan);
        }
    }

    async publish(queueName: string, message: any): Promise<void> {
        const chan = this.publisherChannels[this.nextPublisherIdx];
        this.nextPublisherIdx = (this.nextPublisherIdx + 1) % this.publisherChannels.length;

        const content = Buffer.from(JSON.stringify(message));
        return new Promise<void>((resolve, reject) => {
            chan.publish('benchmark_ex', queueName, content, { persistent: true }, (err: any) => {
                if (err) reject(err);
                else resolve();
            });
        });
    }

    /**
     * RabbitMQ consumer model: each worker unit opens one AMQP subscription per queue.
     * Total open AMQP channels = numWorkers × queues.length.
     * Every queue holds an open channel even when it has zero messages in it —
     * this is the fundamental cost being measured in the sparse-consumer benchmark.
     */
    async startConsumers(
        queues: string[],
        numWorkers: number,
        onMessage: (msg: any, ack: () => Promise<void>) => Promise<void>
    ): Promise<{ totalConsumers: number; description: string }> {
        const totalConsumers = numWorkers * queues.length;
        const maxChannelsPerConnection = 2000;
        const numConnections = Math.ceil(totalConsumers / maxChannelsPerConnection) || 1;

        const baseUrl = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672';
        const consumerUrl = baseUrl.includes('?') ? `${baseUrl}&heartbeat=0` : `${baseUrl}?heartbeat=0`;

        this.consumerConnections = [];
        for (let i = 0; i < numConnections; i++) {
            // Use heartbeat=0 for consumer connections to avoid timeouts during heavy workloads / channel creation
            this.consumerConnections.push(await amqplib.connect(consumerUrl));
        }

        let connIdx = 0;

        for (let w = 0; w < numWorkers; w++) {
            for (const queueName of queues) {
                const conn = this.consumerConnections[connIdx];
                connIdx = (connIdx + 1) % this.consumerConnections.length;

                const channel = await conn.createChannel();
                await channel.prefetch(100);
                await channel.consume(queueName, async (msg: any) => {
                    if (msg) {
                        try {
                            let data = msg.content;
                            if (Buffer.isBuffer(msg.content)) {
                                data = JSON.parse(msg.content.toString());
                            } else if (typeof msg.content === 'string') {
                                data = JSON.parse(msg.content);
                            }
                            await onMessage(data, async () => {
                                channel.ack(msg);
                            });
                        } catch (err) {
                            console.error("Error processing msg", err);
                            channel.nack(msg, false, false);
                        }
                    }
                }, { noAck: false });
            }
        }

        return {
            totalConsumers,
            description: `${numWorkers} workers × ${queues.length} queues = ${totalConsumers} open AMQP channels over ${numConnections} connections`
        };
    }

    async disconnect(): Promise<void> {
        for (const conn of this.consumerConnections) {
            try {
                await conn.close();
            } catch (err) {
                console.error("Error closing consumer connection", err);
            }
        }
        for (const conn of this.publisherConnections) {
            try {
                await conn.close();
            } catch (err) {
                console.error("Error closing publisher connection", err);
            }
        }
        if (this.setupConnection) {
            try {
                await this.setupConnection.close();
            } catch (err) {
                console.error("Error closing setup connection", err);
            }
        }
    }
}
