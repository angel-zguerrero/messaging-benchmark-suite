// @ts-ignore
import { BrokerAsPromised, withDefaultConfig } from 'rascal';
import { IMessagingAdapter } from '../interfaces';

export class RabbitMQAdapter implements IMessagingAdapter {
    private broker!: any;
    private consumerConnections: any[] = [];

    async connect(): Promise<void> {
        // Connection is handled inside setup() via Rascal
    }

    async setup(queueNames: string[]): Promise<void> {
        const baseUrl = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672';
        const url = baseUrl.includes('?') ? `${baseUrl}&heartbeat=0` : `${baseUrl}?heartbeat=0`;

        // Build a single Rascal config that declares all queues, bindings,
        // publications and subscriptions up-front.
        const config = withDefaultConfig({
            vhosts: {
                '/': {
                    connection: {
                        url: url
                    },
                    exchanges: {
                        'benchmark_ex': { assert: true, type: 'topic' }
                    },
                    // Dynamically generate queues, bindings, publications, and subscriptions
                    queues: queueNames.reduce((obj, name) => {
                        obj[name] = {
                            assert: true,
                            options: process.env.QUORUM === 'true' ? { arguments: { 'x-queue-type': 'quorum' } } : {}
                        };
                        return obj;
                    }, {} as Record<string, any>),
                    bindings: queueNames.reduce((obj, name) => {
                        obj[`b1_${name}`] = {
                            source: 'benchmark_ex',
                            destination: name,
                            bindingKey: name
                        };
                        return obj;
                    }, {} as Record<string, any>),
                    publications: queueNames.reduce((obj, name) => {
                        obj[name] = {
                            exchange: 'benchmark_ex',
                            routingKey: name,
                            confirm: true
                        };
                        return obj;
                    }, {} as Record<string, any>),
                    subscriptions: queueNames.reduce((obj, name) => {
                        obj[name] = {
                            queue: name,
                            prefetch: 100
                        };
                        return obj;
                    }, {} as Record<string, any>)
                }
            }
        });

        this.broker = await BrokerAsPromised.create(config);
        this.broker.on('error', (err: any) => console.error("Broker error", err));
    }

    async publish(queueName: string, message: any): Promise<void> {
        const publication = await this.broker.publish(queueName, message);
        return new Promise((resolve, reject) => {
            publication.on('success', () => resolve());
            publication.on('error', reject);
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

        const amqplib = require('amqplib');
        const baseUrl = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672';
        const url = baseUrl.includes('?') ? `${baseUrl}&heartbeat=0` : `${baseUrl}?heartbeat=0`;

        this.consumerConnections = [];
        for (let i = 0; i < numConnections; i++) {
            this.consumerConnections.push(await amqplib.connect(url));
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
        if (this.broker) await this.broker.shutdown();
    }
}
