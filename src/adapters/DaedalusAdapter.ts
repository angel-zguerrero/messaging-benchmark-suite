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

    async setup(queueName: string): Promise<void> {
        await this.sdk.assertTenant({ code: 'benchmark', name: 'Benchmark Tenant' });
        await this.sdk.assertExchange({ tenantCode: 'benchmark', code: 'events', name: 'Events', type: 'topic' });
        await this.sdk.assertQueue({
            tenantCode: 'benchmark',
            code: queueName,
            name: queueName,
            type: 'standard',
            state: 'active',
            vnamespace: 'default',
            allowDuplicated: false,
            maxAttempts: 3,
            priorityType: 'normal'
        });
        await this.sdk.assertBinding({
            code: `bind-${queueName}`,
            tenantCode: 'benchmark',
            exchangeCode: 'events',
            queueCode: queueName,
            pattern: queueName,
            vnamespace: 'default'
        });
    }

    async publish(queueName: string, message: any): Promise<void> {
        await this.sdk.publishMessage({
            tenantCode: 'benchmark',
            exchangeCode: 'events',
            routingKeyOrPatternOrQueueCode: queueName,
            content: JSON.stringify(message),
            vnamespace: 'default'
        });
    }

    async consume(queueName: string, onMessage: (msg: any, ack: () => Promise<void>) => Promise<void>): Promise<void> {
        await this.sdk.createWorker({
            workerName: `bench-worker-${Math.random().toString(36).substring(7)}`,
            intervalMs: 100, // aggressive polling
            capacityPolicies: [
                {
                    maxQueueMessages: 100,
                    claimWorkFilter: {
                        tenantPatterns: ['benchmark'],
                        queueCodes: [queueName]
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

    async disconnect(): Promise<void> {
        // exit process handles this for now as sdk disconnect might not be implemented
    }
}
