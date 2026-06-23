// @ts-ignore
import { BrokerAsPromised, withDefaultConfig } from 'rascal';
import { IMessagingAdapter } from '../interfaces';

export class RabbitMQAdapter implements IMessagingAdapter {
    private broker!: any;

    async connect(): Promise<void> {
        // Handled in setup
    }

    async setup(queueName: string): Promise<void> {
        const config = withDefaultConfig({
            vhosts: {
                '/': {
                    connection: {
                        // Use rabbitmq container name by default
                        url: process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672'
                    },
                    exchanges: {
                        'benchmark_ex': { assert: true, type: 'topic' }
                    },
                    queues: {
                        [queueName]: { 
                            assert: true,
                            options: process.env.QUORUM === 'true' ? { arguments: { 'x-queue-type': 'quorum' } } : {}
                        }
                    },
                    bindings: {
                        [`b1_${queueName}`]: {
                            source: 'benchmark_ex',
                            destination: queueName,
                            bindingKey: queueName
                        }
                    },
                    publications: {
                        [queueName]: {
                            exchange: 'benchmark_ex',
                            routingKey: queueName,
                            confirm: true
                        }
                    },
                    subscriptions: {
                        [queueName]: {
                            queue: queueName,
                            prefetch: 100
                        }
                    }
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

    async consume(queueName: string, onMessage: (msg: any, ack: () => Promise<void>) => Promise<void>): Promise<void> {
        const subscription = await this.broker.subscribe(queueName);
        subscription.on('message', async (message: any, content: any, ackOrNack: any) => {
            try {
                let data = content;
                if (Buffer.isBuffer(content)) {
                    data = JSON.parse(content.toString());
                } else if (typeof content === 'string') {
                    data = JSON.parse(content);
                }
                
                await onMessage(data, async () => {
                    ackOrNack();
                });
            } catch (err) {
                console.error("Error processing msg", err);
                ackOrNack(err, { strategy: 'nack' });
            }
        });
        subscription.on('error', (err: any) => console.error("Subscription err", err));
    }

    async disconnect(): Promise<void> {
        if (this.broker) await this.broker.shutdown();
    }
}
