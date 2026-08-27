export interface IMessagingAdapter {
    connect(): Promise<void>;
    /**
     * Set up one or more queues. Implementations may accept a single name or an array.
     */
    setup(queueNames: string[]): Promise<void>;
    publish(queueName: string, message: any): Promise<void>;
    consume(queueName: string, onMessage: (msg: any, ack: () => Promise<void>) => Promise<void>): Promise<void>;
    disconnect(): Promise<void>;
}
