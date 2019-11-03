import * as pulumi from '@pulumi/pulumi';
import * as gcp from '@pulumi/gcp';

const named = (name: string) => `nestjs-plus-${name}`;

const pubSubAPI = new gcp.projects.Service(named('pubsub-api'), {
  service: 'pubsub.googleapis.com'
});

const pubSubTopicName = named('pubsub-topic');

const pubSubTopic = new gcp.pubsub.Topic(
  pubSubTopicName,
  {
    name: pubSubTopicName
  },
  {
    dependsOn: [pubSubAPI]
  }
);

const pubSubSubscriptionName = named('pubsub-subscription');
const pubSubSubscription = new gcp.pubsub.Subscription(pubSubSubscriptionName, {
  topic: pubSubTopic.name
});

export const PUB_SUB_TOPIC_NAME = pubSubTopic.name;
export const PUB_SUB_SUBSCRIPTION_NAME = pubSubSubscription.name;
