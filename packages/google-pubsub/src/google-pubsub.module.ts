import { PubSub, Message } from '@google-cloud/pubsub';
import { DiscoveryModule, DiscoveryService } from '@nestjs-plus/discovery';
import { DynamicModule, Logger, Module, OnModuleInit } from '@nestjs/common';
import { ExternalContextCreator } from '@nestjs/core/helpers/external-context-creator';
import { groupBy } from 'lodash';
import {
  GOOGLE_PUBSUB_HANDLER,
  GOOGLE_PUBSUB_CONFIG
} from './google-pubsub.constants';
import {
  GooglePubSubHandlerConfig,
  GooglePubSubConfig
} from './google-pubsub.interfaces';
import {
  AsyncOptionsFactoryProvider,
  createAsyncProviders
} from '@nestjs-plus/common';

@Module({
  imports: [DiscoveryModule]
})
export class GooglePubSubModule implements OnModuleInit {
  private readonly logger = new Logger(GooglePubSubModule.name);
  constructor(
    private readonly discover: DiscoveryService,
    private readonly externalContextCreator: ExternalContextCreator,
    private readonly pubSub: PubSub
  ) {}

  public static forRootAsync(
    asyncOptionsFactoryProvider: AsyncOptionsFactoryProvider<GooglePubSubConfig>
  ): DynamicModule {
    return {
      module: GooglePubSubModule,
      imports: asyncOptionsFactoryProvider.imports,
      providers: [
        ...createAsyncProviders(
          asyncOptionsFactoryProvider,
          GOOGLE_PUBSUB_CONFIG
        ),
        {
          provide: PubSub,
          useFactory: async (config: GooglePubSubConfig): Promise<PubSub> => {
            const pubSub = new PubSub({ projectId: config.projectId });
            return pubSub;
          },
          inject: [GOOGLE_PUBSUB_CONFIG]
        }
      ]
    };
  }

  public static forRoot(config: GooglePubSubConfig): DynamicModule {
    return {
      module: GooglePubSubModule,
      providers: [
        {
          provide: PubSub,
          useFactory: async (): Promise<PubSub> => {
            // const logger = new Logger(GooglePubSubModule.name);

            const pubSub = new PubSub({ projectId: config.projectId });

            // const [topic] = await pubSub.createTopic('test');
            // console.log(`Topic ${topic.name} created.`);

            // const [result] = await pubSub
            //   .topic('test')
            //   .createSubscription('testSub');

            // console.log(result);

            // const sub = pubSub.subscription('testSub');

            return pubSub;
          }
        }
      ],
      exports: [PubSub]
    };
  }

  public async onModuleInit() {
    this.logger.log('Initializing Google PubSub Connection');

    const pubSubMeta = await this.discover.providerMethodsWithMetaAtKey<
      GooglePubSubHandlerConfig
    >(GOOGLE_PUBSUB_HANDLER);

    const providerGrouped = groupBy(
      pubSubMeta,
      x => x.discoveredMethod.parentClass.name
    );

    const providerKeys = Object.keys(providerGrouped);
    for (const key of providerKeys) {
      this.logger.log(`Registering google pubsub handlers from ${key}`);
      await Promise.all(
        providerGrouped[key].map(async ({ discoveredMethod, meta }) => {
          const handler = this.externalContextCreator.create(
            discoveredMethod.parentClass.instance,
            discoveredMethod.handler,
            discoveredMethod.methodName
          );

          const { topic, subscription } = meta;

          this.logger.log(
            `Attaching subscription handler on topic ${topic} and subscription ${subscription}`
          );

          const pubSubHandler = async (msg: Message) => {
            try {
              const parsed = JSON.parse(msg.data.toString());
              await handler(parsed, msg);
              msg.ack();
            } catch (e) {
              throw e;
            }
          };

          this.pubSub
            .topic(topic)
            .subscription(subscription)
            .on('message', pubSubHandler);
        })
      );
    }
  }
}
