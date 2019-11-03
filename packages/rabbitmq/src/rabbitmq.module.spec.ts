import { Injectable, Module } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { AmqpConnection } from './amqp/connection';
import { RabbitRPC, RabbitSubscribe } from './rabbitmq.decorators';
import { RabbitMQModule } from './rabbitmq.module';

jest.mock('./amqp/connection');

@Injectable()
class ExampleService {
  @RabbitRPC({
    routingKey: 'rpc',
    exchange: 'exchange1'
  })
  rpcMethod() {}

  @RabbitSubscribe({
    routingKey: 'subscribe',
    exchange: 'exchange2'
  })
  subscribeMethod() {}
}

@Module({
  providers: [ExampleService]
})
class ExampleModule {}

describe('RabbitMQModule', () => {
  let app: TestingModule;
  let amqpMock: AmqpConnection;

  describe('Module configuration', () => {});

  describe('Attaching Handlers', () => {
    beforeEach(async () => {
      amqpMock = new AmqpConnection({
        uri: '',
        exchanges: []
      });

      app = await Test.createTestingModule({
        imports: [ExampleModule, RabbitMQModule.attach(amqpMock)]
      }).compile();

      await app.init();
    });

    it('should register rabbit rpc handlers', async () => {
      expect(amqpMock.createRpc).toBeCalledTimes(1);

      expect(amqpMock.createRpc).toBeCalledWith(
        expect.any(Function),
        expect.objectContaining({
          exchange: 'exchange1',
          routingKey: 'rpc'
        })
      );
    });

    it('should register rabbit subscribe handlers', async () => {
      expect(amqpMock.createSubscriber).toBeCalledTimes(1);

      expect(amqpMock.createSubscriber).toBeCalledWith(
        expect.any(Function),
        expect.objectContaining({
          exchange: 'exchange2',
          routingKey: 'subscribe'
        })
      );
    });
  });
});
