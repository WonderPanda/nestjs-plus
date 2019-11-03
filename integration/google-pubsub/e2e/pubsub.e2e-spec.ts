import { PubSub } from '@google-cloud/pubsub';
import { Injectable } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import {
  GooglePubSubHandler,
  GooglePubSubConfig,
  GooglePubSubModule
} from '@nestjs-plus/google-pubsub';

const jestFn = jest.fn();

const topic = process.env.PUB_SUB_TOPIC_NAME;
const subscription = process.env.PUB_SUB_SUBSCRIPTION_NAME;

@Injectable()
class ExampleService {
  @GooglePubSubHandler({
    topic,
    subscription
  })
  method(input: any) {
    jestFn(input);
  }
}

describe('GooglePubSubModule', () => {
  let app: TestingModule;
  let pubSub: PubSub;

  beforeEach(async () => {
    app = await Test.createTestingModule({
      imports: [
        GooglePubSubModule.forRoot({
          projectId: 'nestjs-plus'
        })
      ],
      providers: [ExampleService]
    }).compile();

    await app.init();
    pubSub = app.get<PubSub>(PubSub);
  });

  it('should receive messages over a subscription', async done => {
    const publishResult = await pubSub.topic(topic).publishJSON({
      body: 42
    });

    console.log(publishResult);

    setTimeout(() => {
      expect(jestFn).toHaveBeenCalledTimes(1);
      done();
    }, 3000);
  });
});
