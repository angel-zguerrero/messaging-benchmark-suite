import { Queue, Worker } from 'bullmq';
import Redis from 'ioredis';
import { IMessagingAdapter } from '../interfaces';

export class BullMQAdapter implements IMessagingAdapter {
    private redisConnection!: Redis;
    private queues: Map<string, Queue> = new Map();
    private workers: Worker[] = [];

    async connect(): Promise<void> {
        const redisUrl = process.env.REDIS_URL || 'redis://redis:6379';
        this.redisConnection = new Redis(redisUrl, {
            maxRetriesPerRequest: null
        });
    }

    async setup(queueNames: string[]): Promise<void> {
        for (const queueName of queueNames) {
            const queue = new Queue(queueName, {
                connection: this.redisConnection,
                defaultJobOptions: {
                    removeOnComplete: true,
                    removeOnFail: true
                }
            });
            this.queues.set(queueName, queue);
        }
    }

    async publish(queueName: string, message: any): Promise<void> {
        const queue = this.queues.get(queueName);
        if (!queue) {
            throw new Error(`Queue ${queueName} not setup`);
        }
        // Providing explicit jobId avoids an extra Redis INCR round-trip during queue.add()
        const jobId = `${Date.now()}:${Math.random().toString(36).substring(2, 9)}`;
        await queue.add('benchmark_job', message, { jobId });
    }

    async startConsumers(
        queues: string[],
        numWorkers: number,
        onMessage: (msg: any, ack: () => Promise<void>) => Promise<void>
    ): Promise<{ totalConsumers: number; description: string }> {
        const redisUrl = process.env.REDIS_URL || 'redis://redis:6379';
        const totalConsumers = numWorkers * queues.length;

        for (let w = 0; w < numWorkers; w++) {
            for (const queueName of queues) {
                const worker = new Worker(
                    queueName,
                    async (job) => {
                        await onMessage(job.data, async () => {
                            // BullMQ automatically completes/acks jobs when worker function resolves successfully
                        });
                    },
                    {
                        connection: new Redis(redisUrl, { maxRetriesPerRequest: null }),
                        concurrency: 1
                    }
                );
                this.workers.push(worker);
            }
        }

        return {
            totalConsumers,
            description: `${numWorkers} workers × ${queues.length} queues = ${totalConsumers} BullMQ workers`
        };
    }

    async disconnect(): Promise<void> {
        for (const worker of this.workers) {
            try {
                await worker.close();
            } catch (err) {
                console.error("Error closing BullMQ worker", err);
            }
        }
        for (const queue of this.queues.values()) {
            try {
                await queue.close();
            } catch (err) {
                console.error("Error closing BullMQ queue", err);
            }
        }
        if (this.redisConnection) {
            try {
                await this.redisConnection.quit();
            } catch (err) {
                console.error("Error quitting Redis connection", err);
            }
        }
    }
}
