import { Injectable } from '@nestjs/common';
import { Kafka } from 'kafkajs';

@Injectable()
export class KafkaUtils {
    async runConsumer({ kafka, group }: { kafka: Kafka; group: string }) {
        const consumer = kafka.consumer({ groupId: group });
        await consumer.connect();

        await consumer.subscribe({
            topics: ['rider', 'driver'],
            fromBeginning: true,
        });

        await consumer.run({
            eachMessage: async ({
                topic,
                partition,
                message,
                heartbeat,
                pause,
            }) => {
                console.log(
                    `${group}: [${topic}]: PART:${partition}:`,
                    message.value.toString()
                );
            },
        });
    }
}
