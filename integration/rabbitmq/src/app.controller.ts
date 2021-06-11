import { AmqpConnection } from '@golevelup/nestjs-rabbitmq';
import { Controller, Get } from '@nestjs/common';

export interface MessageSchema {
  userId: string;
  customField: string;
}

@Controller()
export class AppController {
  constructor(private readonly amqpConnection: AmqpConnection) {}

  @Get()
  getHello(): string {
    return 'Hello World!';
  }

  @Get('rpc')
  async getRpc() {
    return this.amqpConnection.request({
      exchange: 'exchange1',
      routingKey: 'rpc',
    });
  }

  @Get('setup-custom-metric-consumer')
  async setupCustomMetricConsumer() {
    return this.amqpConnection.createSubscriber<MessageSchema>(
      (msg, rawMessage) => {
        // tslint:disable-next-line:no-console
        console.log(
          `Parsed message: ${JSON.stringify(
            msg,
          )}, Raw message: ${rawMessage.content.toString()}`,
        );
        return undefined;
      },
      {
        exchange: 'customExchange',
        routingKey: '',
        messageParser: (message: string): MessageSchema => ({
          userId: message.split('|')[0],
          customField: message.split('|')[1],
        }),
      },
    );
  }

  @Get('publish-custom-metric')
  async publishCustomMetric() {
    const message: MessageSchema = {
      userId: 'uuid',
      customField: 'custom value',
    };

    await this.amqpConnection.publish('customExchange', '', message);

    return message;
  }
}
