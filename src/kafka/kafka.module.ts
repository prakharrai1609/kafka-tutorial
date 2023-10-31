import { Module } from '@nestjs/common';
import { KafkaService } from './kafka.service';
import { KafkaController } from './kafka.controller';
import { KafkaUtils } from './kafka.utils';

@Module({
    providers: [KafkaService, KafkaUtils],
    controllers: [KafkaController],
})
export class KafkaModule {}
