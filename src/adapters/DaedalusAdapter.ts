import { DaedalusSDK } from '@omicron-x/daedalus-sdk';
import { IMessagingAdapter } from '../interfaces';

export class DaedalusAdapter implements IMessagingAdapter {
    private sdk: DaedalusSDK;

    constructor() {
        this.sdk = new DaedalusSDK({
            uri: process.env.DAEDALUS_URL || 'http://daedalus:4000',
            username: 'admin',
            password: 'admin'
        });
    }

    async connect(): Promise<void> {
        await this.sdk.connect();
    }

    async setup(queueNames: string[]): Promise<void> {
        // Tenant and exchange are asserted once for the whole benchmark run
        await this.sdk.assertTenant({ code: 'benchmark', name: 'Benchmark Tenant' });
        await this.sdk.assertExchange({ tenantCode: 'benchmark', code: 'events', name: 'Events', type: 'topic' });
        const queuesConfig = queueNames.map(q => ({
            tenantCode: 'benchmark',
            code: q,
            name: q,
            type: 'standard',
            state: 'active',
            vnamespace: 'default',
            allowDuplicated: false,
            maxAttempts: 3
        }));

        const QUEUE_BATCH_SIZE = 200;
        for (let i = 0; i < queuesConfig.length; i += QUEUE_BATCH_SIZE) {
            const batch = queuesConfig.slice(i, i + QUEUE_BATCH_SIZE);
            await this.sdk.bulkAssertQueues({
                tenantCode: 'benchmark',
                queues: batch
            });
        }

        const bindingsConfig = queueNames.map(queueName => ({
            code: `bind-${queueName}`,
            tenantCode: 'benchmark',
            exchangeCode: 'events',
            queueCode: queueName,
            pattern: queueName,
            routingKey: queueName,
            vnamespace: 'default'
        }));

        const BINDING_BATCH_SIZE = 200;
        for (let i = 0; i < bindingsConfig.length; i += BINDING_BATCH_SIZE) {
            const batch = bindingsConfig.slice(i, i + BINDING_BATCH_SIZE);
            await this.sdk.bulkAssertBindings({
                tenantCode: 'benchmark',
                bindings: batch
            });
        }
    }

    async publish(queueName: string, message: any): Promise<void> {
        await this.sdk.publishMessage({
            tenantCode: 'benchmark',
            exchangeCode: 'events',
            routingKeyOrPatternOrQueueCode: queueName,
            content: JSON.stringify(message),
            vnamespace: 'default',
            options: {
                waitForConfirmation: true,
                timeoutMs: 60000
            }
        });
    }

    /**
     * Daedalus consumer model: numWorkers polling workers are created, each covering ALL queues.
     * Total workers = numWorkers, regardless of how many queues are declared.
     *
     * A worker does not hold a persistent channel per queue. When a queue has no messages,
     * it costs nothing — it simply lives in Pebble (disk). The worker will detect new tasks
     * on the next polling interval.
     *
     * This is the architectural contrast with RabbitMQ:
     *   - RabbitMQ needs numWorkers × queues.length open AMQP channels.
     *   - Daedalus needs exactly numWorkers workers, always, regardless of queue count.
     */
    async startConsumers(
        queues: string[],
        numWorkers: number,
        onMessage: (msg: any, ack: () => Promise<void>) => Promise<void>
    ): Promise<{ totalConsumers: number; description: string }> {
        for (let w = 0; w < numWorkers; w++) {
            await this.sdk.createWorker({
                workerName: `bench-worker-${w}-${Math.random().toString(36).substring(7)}`,
                intervalMs: 10, // poll all queues every 100ms
                capacityPolicies: [
                    {
                        // Each worker can claim up to 300 messages per cycle from ANY queue
                        maxQueueMessages: 2_000,
                        claimWorkFilter: {
                            tenantPatterns: ['benchmark'],
                            queueCodes: queues // covers ALL declared queues
                        }
                    }
                ],
                onMessage: async (message: any, ack: any) => {
                    await onMessage(JSON.parse(message.message.content), async () => {
                        await ack();
                    });
                }
            });
        }

        return {
            totalConsumers: numWorkers,
            description: `${numWorkers} workers (each polling all ${queues.length} queues)`
        };
    }

    async disconnect(): Promise<void> {
        // SDK disconnect is handled by the process exit
    }
}
