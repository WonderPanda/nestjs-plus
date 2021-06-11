/**
 * A function that can be used to serialize messages before they are sent over RabbitMQ
 */
export type AmqpMessageSerializer = (message: any) => Buffer;

/**
 * A function that can be used to deserialize messages received through RabbitMQ
 */
export type AmqpMessageDeserializer<T = any> = (message: Buffer) => T;

/**
 * A deserializer that will convert a raw RabbitMQ message into a JSON object
 */
export const jsonMessageDeserializer: AmqpMessageDeserializer = <T>(
  message: Buffer
): T => JSON.parse(message.toString());

/**
 * A serializer that will convert a JSON object into string format to be sent over RabbitMQ
 */
export const jsonMessageSerializer: AmqpMessageSerializer = (message) =>
  Buffer.from(JSON.stringify(message));

/**
 * A general purpose message serializer that can handle a variety of inputs to safely
 * pass messages through RabbitMQ
 */
export const defaultMessageSerializer: AmqpMessageSerializer = (message) => {
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

  return buffer;
};
