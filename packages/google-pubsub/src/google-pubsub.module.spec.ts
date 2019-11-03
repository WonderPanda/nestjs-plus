import { PubSub } from '@google-cloud/pubsub';
import { Injectable } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { GooglePubSubHandler } from './google-pubsub.decorators';
import { GooglePubSubModule } from './google-pubsub.module';

const jestFn = jest.fn();

const topic = 'test';
const subscription = 'testSub';

@Injectable()
class ExampleService {
  @GooglePubSubHandler({
    topic,
    subscription
  })
  method(input: any) {
    console.log(input);
    jestFn(input);
  }
}

describe('GooglePubSubModule', () => {
  let app: TestingModule;
  let pubSub: PubSub;

  beforeEach(async () => {
    app = await Test.createTestingModule({
      imports: [GooglePubSubModule.forRoot()],
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
      expect(jestFn).toHaveBeenCalled();
      done();
    }, 3000);
  });
});
