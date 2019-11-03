import { DiscoveryModule, DiscoveryService } from '@nestjs-plus/discovery';
import {
  DynamicModule,
  Logger,
  Module,
  OnModuleInit,
  Provider
} from '@nestjs/common';
import {
  AsyncOptionsFactoryProvider,
  createAsyncProviders
} from '@nestjs-plus/common';
import { ExternalContextCreator } from '@nestjs/core/helpers/external-context-creator';
import { groupBy } from 'lodash';
import { AmqpConnection } from './amqp/connection';
import { RABBIT_HANDLER, RABBIT_CONFIG } from './rabbitmq.constants';
import { RabbitHandlerConfig, RabbitMQConfig } from './rabbitmq.interfaces';

@Module({
  imports: [DiscoveryModule]
})
export class RabbitMQModule implements OnModuleInit {
  private readonly logger = new Logger(RabbitMQModule.name);

  constructor(
    private readonly discover: DiscoveryService,
    private readonly amqpConnection: AmqpConnection,
    private readonly externalContextCreator: ExternalContextCreator
  ) {}

  public static forRootAsync(
    asyncOptionsFactoryProvider: AsyncOptionsFactoryProvider<RabbitMQConfig>
  ): DynamicModule {
    return {
      module: RabbitMQModule,
      exports: [AmqpConnection],
      imports: asyncOptionsFactoryProvider.imports,
      providers: [
        ...createAsyncProviders(asyncOptionsFactoryProvider, RABBIT_CONFIG),
        {
          provide: AmqpConnection,
          useFactory: async (
            config: RabbitMQConfig
          ): Promise<AmqpConnection> => {
            const connection = new AmqpConnection(config);
            await connection.init();
            const logger = new Logger(RabbitMQModule.name);
            logger.log('Successfully connected to RabbitMQ');
            return connection;
          },
          inject: [RABBIT_CONFIG]
        }
      ]
    };
  }

  public static forRoot(config: RabbitMQConfig): DynamicModule {
    return {
      module: RabbitMQModule,
      providers: [
        {
          provide: AmqpConnection,
          useFactory: async (): Promise<AmqpConnection> => {
            const connection = new AmqpConnection(config);
            await connection.init();
            const logger = new Logger(RabbitMQModule.name);
            logger.log('Successfully connected to RabbitMQ');
            return connection;
          }
        }
      ],
      exports: [AmqpConnection]
    };
  }

  public static build(config: RabbitMQConfig): DynamicModule {
    const logger = new Logger(RabbitMQModule.name);
    logger.warn(
      'build() is deprecated. use forRoot() or forRootAsync() to configure RabbitMQ'
    );
    return {
      module: RabbitMQModule,
      providers: [
        {
          provide: AmqpConnection,
          useFactory: async (): Promise<AmqpConnection> => {
            const connection = new AmqpConnection(config);
            await connection.init();
            const logger = new Logger(RabbitMQModule.name);
            logger.log('Successfully connected to RabbitMQ');
            return connection;
          }
        }
      ],
      exports: [AmqpConnection]
    };
  }

  public static attach(connection: AmqpConnection): DynamicModule {
    return {
      module: RabbitMQModule,
      providers: [
        {
          provide: AmqpConnection,
          useValue: connection
        }
      ],
      exports: [AmqpConnection]
    };
  }

  public async onModuleInit() {
    this.logger.log('Initializing RabbitMQ Handlers');

    const rabbitMeta = await this.discover.providerMethodsWithMetaAtKey<
      RabbitHandlerConfig
    >(RABBIT_HANDLER);

    const grouped = groupBy(
      rabbitMeta,
      x => x.discoveredMethod.parentClass.name
    );

    const providerKeys = Object.keys(grouped);
    for (const key of providerKeys) {
      this.logger.log(`Registering rabbitmq handlers from ${key}`);
      await Promise.all(
        grouped[key].map(async ({ discoveredMethod, meta: config }) => {
          const handler = this.externalContextCreator.create(
            discoveredMethod.parentClass.instance,
            discoveredMethod.handler,
            discoveredMethod.methodName
          );

          const { exchange, routingKey, queue } = config;

          this.logger.log(
            `${discoveredMethod.parentClass.name}.${
              discoveredMethod.methodName
            } {${config.type}} -> ${exchange}::${routingKey}::${queue ||
              'amqpgen'}`
          );

          return config.type === 'rpc'
            ? this.amqpConnection.createRpc(handler, config)
            : this.amqpConnection.createSubscriber(handler, config);
        })
      );
    }
  }
}
