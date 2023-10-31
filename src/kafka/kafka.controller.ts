import { Body, Controller, Get, Param, Post } from '@nestjs/common';
import { Kafka } from 'kafkajs';
import { KafkaService } from './kafka.service';

@Controller('kafka')
export class KafkaController {
    private admin: Kafka = null;
    constructor(private kafkaService: KafkaService) {
        this.init();
    }

    async init(): Promise<void> {
        if (!this.admin) {
            this.admin = await this.kafkaService.getKafkaClient();
        }
    }

    @Get()
    healthCheck(): { status: number; message: string } {
        return {
            status: 201,
            message: 'kafka route is healthy...',
        };
    }

    @Post('produce')
    async connectAndProduce(
        @Body() body: { line: string }
    ): Promise<{
        success: boolean;
        message: {
            topic: string;
            messages: { partition: number; key: string; value: string }[];
        };
    }> {
        const data = await this.kafkaService.producer({
            kafka: this.admin,
            line: body.line,
        });

        return data;
    }

    @Post('consume/:groupId')
    async connectAndConsume(
        @Param('groupId') groupId: string
    ): Promise<{ success: boolean; message: string }> {
        if (!this.admin) {
            throw Error('admin is not initialized...');
        }

        const data = await this.kafkaService.consume({
            kafka: this.admin,
            group: groupId,
        });

        return data;
    }
}
