export interface IMessagingAdapter {
    connect(): Promise<void>;
    /**
     * Declare all queues on the broker. Both brokers pay the same "declared queue" cost here.
     */
    setup(queueNames: string[]): Promise<void>;
    publish(queueName: string, message: any): Promise<void>;
    /**
     * Start `numWorkers` concurrent consumer units that collectively cover all given queues.
     *
     * The semantics differ by broker — this is intentional and IS the thing being benchmarked:
     *
     *   RabbitMQ  — each of the numWorkers units opens one AMQP subscription per queue.
     *               Total open channels = numWorkers × queues.length.
     *               Every queue has a dedicated channel even when it has zero messages.
     *
     *   Daedalus  — numWorkers polling workers are created, each with a claimWorkFilter
     *               that covers ALL queues. Total workers = numWorkers, regardless of queue count.
     *               Queues with no messages have zero live connections; they stay on Pebble disk.
     */
    startConsumers(
        queues: string[],
        numWorkers: number,
        onMessage: (msg: any, ack: () => Promise<void>) => Promise<void>
    ): Promise<{ totalConsumers: number; description: string }>;
    disconnect(): Promise<void>;
}
