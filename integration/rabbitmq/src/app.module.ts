import { RabbitMQModule } from '@golevelup/nestjs-rabbitmq';
import { Module } from '@nestjs/common';
import { AppController, MessageSchema } from './app.controller';
import { RpcService } from './rpc/rpc.service';

const rabbitHost = process.env.NODE_ENV === 'ci' ? 'rabbit' : 'localhost';

const customExchangeSerializer = (message: MessageSchema) =>
  Object.values(message).join('|');

@Module({
  imports: [
    RabbitMQModule.forRootAsync(RabbitMQModule, {
      useFactory: () => ({
        exchanges: [
          {
            name: 'exchange1',
            type: 'topic',
          },
          {
            name: 'customExchange',
            type: 'fanout',
            messageSerializer: customExchangeSerializer,
          },
        ],
        uri: `amqp://rabbitmq:rabbitmq@${rabbitHost}:5672`,
        connectionInitOptions: { wait: true, reject: true, timeout: 3000 },
      }),
    }),
  ],
  controllers: [AppController],
  providers: [RpcService],
})
export class AppModule {}
