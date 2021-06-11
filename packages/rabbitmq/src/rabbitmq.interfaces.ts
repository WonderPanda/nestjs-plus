import * as amqpConnectionManager from 'amqp-connection-manager';
import * as amqplib from 'amqplib';
import {
  MessageErrorHandler,
  MessageHandlerErrorBehavior,
} from './amqp/errorBehaviors';
import {
  AmqpMessageDeserializer,
  AmqpMessageSerializer,
} from './amqp/serialization';

export interface RabbitMQExchangeConfig {
  name: string;
  type?: string;
  options?: amqplib.Options.AssertExchange;
  messageSerializer?: AmqpMessageSerializer;
}

export interface RequestOptions {
  exchange: string;
  routingKey: string;
  correlationId?: string;
  timeout?: number;
  payload?: any;
  messageDeserializer?: AmqpMessageDeserializer<any>;
}

export interface QueueOptions {
  durable?: boolean;
  exclusive?: boolean;
  autoDelete?: boolean;
  arguments?: any;
  messageTtl?: number;
  expires?: number;
  deadLetterExchange?: string;
  deadLetterRoutingKey?: string;
  maxLength?: number;
  maxPriority?: number;
}

export interface MessageHandlerOptions {
  exchange: string;
  routingKey: string | string[];
  queue?: string;
  queueOptions?: QueueOptions;
  /**
   * @deprecated()
   * Legacy error handling behaviors. This will be overridden if the errorHandler property is set
   */
  errorBehavior?: MessageHandlerErrorBehavior;
  /**
   * A function that will be called if an error is thrown during processing of an incoming message
   */
  errorHandler?: MessageErrorHandler;
  allowNonJsonMessages?: boolean;
  messageDeserializer?: AmqpMessageDeserializer<any>;
}

export interface ConnectionInitOptions {
  wait?: boolean;
  timeout?: number;
  reject?: boolean;
}

export interface RabbitMQConfig {
  uri: string | string[];
  prefetchCount?: number;
  exchanges?: RabbitMQExchangeConfig[];
  defaultRpcTimeout?: number;
  defaultExchangeType?: string;
  defaultRpcErrorBehavior?: MessageHandlerErrorBehavior;
  defaultSubscribeErrorBehavior?: MessageHandlerErrorBehavior;
  connectionInitOptions?: ConnectionInitOptions;
  connectionManagerOptions?: amqpConnectionManager.AmqpConnectionManagerOptions;
  registerHandlers?: boolean;
  enableDirectReplyTo?: boolean;
}

export type RabbitHandlerType = 'rpc' | 'subscribe';

export interface RabbitHandlerConfig extends MessageHandlerOptions {
  type: RabbitHandlerType;
}
