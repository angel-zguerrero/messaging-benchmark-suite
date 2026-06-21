export interface IMessagingAdapter {
    connect(): Promise<void>;
    setup(queueName: string): Promise<void>;
    publish(queueName: string, message: any): Promise<void>;
    consume(queueName: string, onMessage: (msg: any, ack: () => Promise<void>) => Promise<void>): Promise<void>;
    disconnect(): Promise<void>;
}
