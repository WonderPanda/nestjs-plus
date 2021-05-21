import { Logger } from '@nestjs/common';
import * as amqpcon from 'amqp-connection-manager';
import * as amqplib from 'amqplib';
import { EMPTY, interval, race, Subject, throwError } from 'rxjs';
import {
  catchError,
  filter,
  first,
  map,
  take,
  timeoutWith,
} from 'rxjs/operators';
import * as uuid from 'uuid';
import {
  ConnectionInitOptions,
  MessageHandlerOptions,
  RabbitMQConfig,
  RequestOptions,
} from '../rabbitmq.interfaces';
import {
  getHandlerForLegacyBehavior,
  MessageHandlerErrorBehavior,
} from './errorBehaviors';
import { Nack, RpcResponse, SubscribeResponse } from './handlerResponses';
import { inspect } from 'util';

const DIRECT_REPLY_QUEUE = 'amq.rabbitmq.reply-to';

export interface CorrelationMessage {
  correlationId: string;
  message: {};
}

const defaultConfig = {
  prefetchCount: 10,
  defaultExchangeType: 'topic',
  defaultRpcErrorBehavior: MessageHandlerErrorBehavior.REQUEUE,
  defaultSubscribeErrorBehavior: MessageHandlerErrorBehavior.REQUEUE,
  exchanges: [],
  defaultRpcTimeout: 10000,
  connectionInitOptions: {
    wait: true,
    timeout: 5000,
    reject: true,
  },
  connectionManagerOptions: {},
  registerHandlers: true,
  enableDirectReplyTo: true,
};

export class AmqpConnection {
  private readonly messageSubject = new Subject<CorrelationMessage>();
  private readonly config: Required<RabbitMQConfig>;
  private readonly logger: Logger;
  private readonly initialized = new Subject();
  private _managedConnection!: amqpcon.AmqpConnectionManager;
  private _managedChannel!: amqpcon.ChannelWrapper;
  private _channel?: amqplib.Channel;
  private _connection?: amqplib.Connection;

  constructor(config: RabbitMQConfig) {
    this.config = { ...defaultConfig, ...config };
    this.logger = new Logger(AmqpConnection.name);
  }

  get channel(): amqplib.Channel {
    if (!this._channel) throw new Error('channel is not available');
    return this._channel;
  }

  get connection(): amqplib.Connection {
    if (!this._connection) throw new Error('connection is not available');
    return this._connection;
  }

  get managedChannel(): amqpcon.ChannelWrapper {
    return this._managedChannel;
  }

  get managedConnection(): amqpcon.AmqpConnectionManager {
    return this._managedConnection;
  }

  get configuration() {
    return this.config;
  }

  public async init(): Promise<void> {
    const options: Required<ConnectionInitOptions> = {
      ...defaultConfig.connectionInitOptions,
      ...this.config.connectionInitOptions,
    };

    const { wait, timeout: timeoutInterval, reject } = options;

    const p = this.initCore();
    if (!wait) return p;

    return this.initialized
      .pipe(
        take(1),
        timeoutWith(
          timeoutInterval,
          throwError(
            new Error(
              `Failed to connect to a RabbitMQ broker within a timeout of ${timeoutInterval}ms`
            )
          )
        ),
        catchError((err) => (reject ? throwError(err) : EMPTY))
      )
      .toPromise<any>();
  }

  private async initCore(): Promise<void> {
    this.logger.log('Trying to connect to a RabbitMQ broker');

    this._managedConnection = amqpcon.connect(
      Array.isArray(this.config.uri) ? this.config.uri : [this.config.uri],
      this.config.connectionManagerOptions
    );

    this._managedConnection.on('connect', ({ connection }) => {
      this._connection = connection;
      this.logger.log('Successfully connected to a RabbitMQ broker');
    });

    this._managedConnection.on('disconnect', ({ err }) => {
      this.logger.error('Disconnected from RabbitMQ broker', err?.stack);
    });

    this._managedChannel = this._managedConnection.createChannel({
      name: AmqpConnection.name,
    });

    this._managedChannel.on('connect', () =>
      this.logger.log('Successfully connected a RabbitMQ channel')
    );

    this._managedChannel.on('error', (err, { name }) =>
      this.logger.log(
        `Failed to setup a RabbitMQ channel - name: ${name} / error: ${err.message} ${err.stack}`
      )
    );

    this._managedChannel.on('close', () =>
      this.logger.log('Successfully closed a RabbitMQ channel')
    );

    await this._managedChannel.addSetup((c) => this.setupInitChannel(c));
  }

  private async setupInitChannel(
    channel: amqplib.ConfirmChannel
  ): Promise<void> {
    this._channel = channel;

    this.config.exchanges.forEach(async (x) =>
      channel.assertExchange(
        x.name,
        x.type || this.config.defaultExchangeType,
        x.options
      )
    );

    await channel.prefetch(this.config.prefetchCount);

    if (this.config.enableDirectReplyTo) {
      await this.initDirectReplyQueue(channel);
    }

    this.initialized.next();
  }

  private async initDirectReplyQueue(channel: amqplib.ConfirmChannel) {
    this.logger.verbose(
      'Set up a consumer on the Direct Reply-To queue to facilitate RPC functionality'
    );
    // Set up a consumer on the Direct Reply-To queue to facilitate RPC functionality
    await channel.consume(
      DIRECT_REPLY_QUEUE,
      async (msg) => {
        if (msg == null) {
          return;
        }

        const correlationMessage: CorrelationMessage = {
          correlationId: msg.properties.correlationId.toString(),
          message: JSON.parse(msg.content.toString()),
        };

        this.messageSubject.next(correlationMessage);
      },
      {
        noAck: true,
      }
    );
  }

  public async request<T extends {}>(
    requestOptions: RequestOptions
  ): Promise<T> {
    const correlationId = requestOptions.correlationId || uuid.v4();
    const timeout = requestOptions.timeout || this.config.defaultRpcTimeout;
    const payload = requestOptions.payload || {};

    const response$ = this.messageSubject.pipe(
      filter((x) => x.correlationId === correlationId),
      map((x) => x.message as T),
      first()
    );

    await this.publish(
      requestOptions.exchange,
      requestOptions.routingKey,
      payload,
      {
        replyTo: DIRECT_REPLY_QUEUE,
        correlationId,
      }
    );

    const timeout$ = interval(timeout).pipe(
      first(),
      map(() => {
        throw new Error(
          `Failed to receive response within timeout of ${timeout}ms`
        );
      })
    );

    return race(response$, timeout$).toPromise();
  }

  public async createSubscriber<T>(
    handler: (
      msg: T | undefined,
      rawMessage?: amqplib.ConsumeMessage
    ) => Promise<SubscribeResponse>,
    msgOptions: MessageHandlerOptions
  ) {
    return this._managedChannel.addSetup((channel) =>
      this.setupSubscriberChannel<T>(handler, msgOptions, channel)
    );
  }

  private async setupSubscriberChannel<T>(
    handler: (
      msg: T | undefined,
      rawMessage?: amqplib.ConsumeMessage
    ) => Promise<SubscribeResponse>,
    msgOptions: MessageHandlerOptions,
    channel: amqplib.ConfirmChannel
  ): Promise<void> {
    const { exchange, routingKey, allowNonJsonMessages } = msgOptions;

    const { queue } = await channel.assertQueue(
      msgOptions.queue || '',
      msgOptions.queueOptions || undefined
    );

    const routingKeys = Array.isArray(routingKey) ? routingKey : [routingKey];

    await Promise.all(
      routingKeys.map((x) => channel.bindQueue(queue, exchange, x))
    );

    await channel.consume(queue, async (msg) => {
      try {
        this.logger.verbose(`Consuming message: ${inspect(msg)}`);
        if (msg == null) {
          this.logger.verbose('Message is null, throwing error');
          throw new Error('Received null message');
        }

        const response = await this.handleMessage(
          handler,
          msg,
          allowNonJsonMessages
        );

        if (response instanceof Nack) {
          this.logger.verbose(
            'Received Nack response, sending nack to channel'
          );
          channel.nack(msg, false, response.requeue);
          return;
        }

        if (response) {
          this.logger.verbose(
            'Received response from subscribe handler. Subscribe handlers should only return void'
          );
          throw new Error(
            'Received response from subscribe handler. Subscribe handlers should only return void'
          );
        }

        channel.ack(msg);
      } catch (e) {
        this.logger.verbose(
          `Got an error when handling message: ${inspect(e)}`
        );
        if (msg == null) {
          return;
        } else {
          const errorHandler =
            msgOptions.errorHandler ||
            getHandlerForLegacyBehavior(
              msgOptions.errorBehavior ||
                this.config.defaultSubscribeErrorBehavior
            );

          await errorHandler(channel, msg, e);
        }
      }
    });
  }

  public async createRpc<T, U>(
    handler: (
      msg: T | undefined,
      rawMessage?: amqplib.ConsumeMessage
    ) => Promise<RpcResponse<U>>,
    rpcOptions: MessageHandlerOptions
  ) {
    return this._managedChannel.addSetup((channel) =>
      this.setupRpcChannel<T, U>(handler, rpcOptions, channel)
    );
  }

  public async setupRpcChannel<T, U>(
    handler: (
      msg: T | undefined,
      rawMessage?: amqplib.ConsumeMessage
    ) => Promise<RpcResponse<U>>,
    rpcOptions: MessageHandlerOptions,
    channel: amqplib.ConfirmChannel
  ) {
    const { exchange, routingKey, allowNonJsonMessages } = rpcOptions;

    const { queue } = await channel.assertQueue(
      rpcOptions.queue || '',
      rpcOptions.queueOptions || undefined
    );

    const routingKeys = Array.isArray(routingKey) ? routingKey : [routingKey];

    await Promise.all(
      routingKeys.map((x) => channel.bindQueue(queue, exchange, x))
    );

    await channel.consume(queue, async (msg) => {
      try {
        this.logger.verbose(`Consuming RPC message: ${inspect(msg)}`);
        if (msg == null) {
          this.logger.verbose('Message is null, throwing error');
          throw new Error('Received null message');
        }

        const response = await this.handleMessage(
          handler,
          msg,
          allowNonJsonMessages
        );

        if (response instanceof Nack) {
          this.logger.verbose(
            'Received Nack response, sending nack to channel'
          );
          channel.nack(msg, false, response.requeue);
          return;
        }

        const { replyTo, correlationId } = msg.properties;
        if (replyTo) {
          this.logger.verbose(
            `reply to is enabled, sending response with correlationId ${correlationId}`
          );
          this.publish('', replyTo, response, { correlationId });
        }
        channel.ack(msg);
      } catch (e) {
        this.logger.verbose(
          `Got an error when handling message: ${inspect(e)}`
        );
        if (msg == null) {
          return;
        } else {
          const errorHandler =
            rpcOptions.errorHandler ||
            getHandlerForLegacyBehavior(
              rpcOptions.errorBehavior ||
                this.config.defaultSubscribeErrorBehavior
            );

          await errorHandler(channel, msg, e);
        }
      }
    });
  }

  public async publish(
    exchange: string,
    routingKey: string,
    message: any,
    options?: amqplib.Options.Publish
  ) {
    // source amqplib channel is used directly to keep the behavior of throwing connection related errors
    if (!this.managedConnection.isConnected() || !this._channel) {
      throw new Error('AMQP connection is not available');
    }

    let buffer: Buffer;
    if (message instanceof Buffer) {
      buffer = message;
    } else if (message instanceof Uint8Array) {
      buffer = Buffer.from(message);
    } else if (message != null) {
      buffer = Buffer.from(JSON.stringify(message));
    } else {
      buffer = Buffer.alloc(0);
    }

    this._channel.publish(exchange, routingKey, buffer, options);
  }

  private handleMessage<T, U>(
    handler: (
      msg: T | undefined,
      rawMessage?: amqplib.ConsumeMessage
    ) => Promise<U>,
    msg: amqplib.ConsumeMessage,
    allowNonJsonMessages?: boolean
  ) {
    let message: T | undefined = undefined;
    if (msg.content) {
      if (allowNonJsonMessages) {
        try {
          message = JSON.parse(msg.content.toString()) as T;
        } catch {
          // Let handler handle parsing error, it has the raw message anyway
          message = undefined;
        }
      } else {
        message = JSON.parse(msg.content.toString()) as T;
      }
    }

    return handler(message, msg);
  }
}
