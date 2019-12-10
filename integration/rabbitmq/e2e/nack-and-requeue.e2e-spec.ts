import {
  AmqpConnection,
  Nack,
  RabbitMQModule,
  RabbitSubscribe,
} from '@golevelup/nestjs-rabbitmq';
import { INestApplication, Injectable } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import * as amqplib from 'amqplib';

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

const exchange = 'testSubscribeExhange';
const nackRoutingKey = 'nackRoutingKey';
const nackAndRequeueRoutingKey = 'nackAndRequeueRoutingKey';
const testMessage = {
  messageProp: 42,
};

const nackHandler = jest.fn();
const nackAndRequeueHandler = jest.fn();

@Injectable()
class SubscribeService {
  nackCount = 0;

  @RabbitSubscribe({
    exchange,
    routingKey: nackRoutingKey,
    queue: nackRoutingKey,
  })
  shouldNack(message: object, raw: amqplib.ConsumeMessage) {
    nackHandler();
    return new Nack();
  }

  @RabbitSubscribe({
    exchange,
    routingKey: nackAndRequeueRoutingKey,
    queue: nackAndRequeueRoutingKey,
  })
  async shouldNackAndRequeueTimes3(
    message: object,
    raw: amqplib.ConsumeMessage,
  ) {
    ++this.nackCount;
    nackAndRequeueHandler();
    // await sleep(15);
    if (this.nackCount >= 3) {
      return new Nack();
    }
    return new Nack(true);
  }
}

describe('Nack and Requeue', () => {
  let app: INestApplication;
  let amqpConnection: AmqpConnection;

  const rabbitHost = process.env.NODE_ENV === 'ci' ? 'rabbit' : 'localhost';
  const uri = `amqp://rabbitmq:rabbitmq@${rabbitHost}:5672`;

  beforeAll(async () => {
    const moduleFixture = await Test.createTestingModule({
      providers: [SubscribeService],
      imports: [
        RabbitMQModule.forRootAsync(RabbitMQModule, {
          useFactory: () => ({
            exchanges: [
              {
                name: exchange,
                type: 'topic',
              },
            ],
            uri,
          }),
        }),
      ],
    }).compile();

    app = moduleFixture.createNestApplication();
    await app.init();
    amqpConnection = app.get<AmqpConnection>(AmqpConnection);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  afterAll(async () => {
    await app.close();
  });

  it('should nack the message when handler returns a Nack object', async () => {
    const spy = jest.spyOn(amqpConnection.rawChannel, 'nack');

    amqpConnection.publish(exchange, nackRoutingKey, { msg: 'nack' });

    await sleep(100);

    expect(nackHandler).toHaveBeenCalledTimes(1);
    expect(spy).toHaveBeenCalledTimes(1);
    expect(spy).toHaveBeenCalledWith(expect.anything(), false, false);
  });

  it('should nack and requeue 3 times', async () => {
    const spy = jest.spyOn(amqpConnection.rawChannel, 'nack');

    amqpConnection.publish(exchange, nackAndRequeueRoutingKey, {
      msg: 'nackAndRequeue',
    });

    await sleep(100);

    expect(nackAndRequeueHandler).toHaveBeenCalledTimes(3);
    expect(spy).toHaveBeenCalledTimes(3);

    expect(spy.mock.calls[0]).toEqual([expect.anything(), false, true]);
    expect(spy.mock.calls[1]).toEqual([expect.anything(), false, true]);
    expect(spy.mock.calls[2]).toEqual([expect.anything(), false, false]);
  });
});
