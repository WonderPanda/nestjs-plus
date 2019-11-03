import { SetMetadata } from '@nestjs/common';
import { GOOGLE_PUBSUB_HANDLER } from './google-pubsub.constants';
import { GooglePubSubHandlerConfig } from './google-pubsub.interfaces';

export const GooglePubSubHandler = (config: GooglePubSubHandlerConfig) =>
  SetMetadata(GOOGLE_PUBSUB_HANDLER, config);
