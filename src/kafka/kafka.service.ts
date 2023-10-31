import { Injectable } from '@nestjs/common';
import { Kafka } from 'kafkajs';
import { KafkaUtils } from './kafka.utils';

@Injectable()
export class KafkaService {
    constructor(private kafkaUtils: KafkaUtils) {}
    async getKafkaClient(): Promise<Kafka> {
        const kafka = new Kafka({
            clientId: 'kafka-client',
            brokers: ['localhost:9092'],
        });

        return kafka;
    }

    async registerKakfaClient(kafka: Kafka) {
        const admin = kafka.admin();
        console.log('Admin connecting...');
        admin.connect();
        console.log('Adming Connection Success...');

        console.log('Creating Topic [driver]');
        await admin.createTopics({
            topics: [
                {
                    topic: 'driver',
                    numPartitions: 2,
                },
            ],
        });
        console.log('Topic Created Success [driver]');

        console.log('Disconnecting Admin..');
        await admin.disconnect();
    }

    async producer({ kafka, line }: { kafka: Kafka; line: string }): Promise<{
        success: boolean;
        message: {
            topic: string;
            messages: { partition: number; key: string; value: string }[];
        };
    }> {
        const producer = kafka.producer();

        console.log('Connecting Producer');
        await producer.connect();
        console.log('Producer Connected Successfully');

        const [riderName, location] = line.split(' ');

        const kafkaMessage = {
            topic: 'driver',
            messages: [
                {
                    partition: location.toLowerCase() === 'north' ? 0 : 1,
                    key: 'location',
                    value: JSON.stringify({ name: riderName, location }),
                },
            ],
        };
        await producer.send(kafkaMessage);

        await producer.disconnect();

        return {
            success: true,
            message: kafkaMessage,
        };
    }

    async consume({
        kafka,
        group,
    }: {
        kafka: Kafka;
        group: string;
    }): Promise<{ success: boolean; message: string }> {
        this.kafkaUtils.runConsumer({ kafka, group });

        return {
            success: true,
            message: 'consumer running...',
        };
    }
}
