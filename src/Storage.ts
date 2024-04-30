import { MessageID, StreamMessage } from '@streamr/protocol';
import { convertBytesToStreamMessage, convertStreamMessageToBytes } from '@streamr/trackerless-network';
import { Readable, Transform, pipeline } from "stream";
import { MAX_SEQUENCE_NUMBER_VALUE, MIN_SEQUENCE_NUMBER_VALUE } from './MessageRef';
import { QueryParams } from "./QueryParams";

export class Storage {
  private readonly data: Uint8Array[];

  constructor(data: Uint8Array[] = []) {
    this.data = data;
  }

  public async store(message: StreamMessage) {
    this.data.push(convertStreamMessageToBytes(message));

    this.data.sort((a: Uint8Array, b: Uint8Array) => {
      const msgA = convertBytesToStreamMessage(a);
      const msgB = convertBytesToStreamMessage(b);

      const msgRefA = msgA.messageId.toMessageRef();
      const msgRefB = msgB.messageId.toMessageRef();

      return msgRefA.compareTo(msgRefB);
    });
  }

  public query(params: QueryParams): Readable {
    return pipeline(
      Readable.from(this.data),
      new Transform({
        objectMode: true,
        transform(chunk, encoding, callback) {
          try {
            const message = convertBytesToStreamMessage(chunk);
            const messageId = message.messageId;

            if (messageId.streamId === params.streamId &&
              (messageId.timestamp > params.from.timestamp ||
                messageId.timestamp === params.from.timestamp &&
                messageId.sequenceNumber >= (params.from.sequenceNumber ?? MIN_SEQUENCE_NUMBER_VALUE)) &&
              (messageId.timestamp < params.to.timestamp ||
                messageId.timestamp === params.to.timestamp &&
                messageId.sequenceNumber <= (params.to.sequenceNumber ?? MAX_SEQUENCE_NUMBER_VALUE)
              )) {
              this.push(message);
            }
            callback();
          } catch (err) {
            callback(err as Error);
          }
        },
      }),
      (err) => {
        if (err) {
          console.error(err);
        }
      }
    );
  }

  public queryByMessageIds(messageIds: MessageID[]): Readable {
    return pipeline(
      Readable.from(this.data),
      new Transform({
        objectMode: true,
        transform(chunk, encoding, callback) {
          try {
            const message = convertBytesToStreamMessage(chunk);
            const messageId = message.messageId;

            if (messageIds.includes(messageId)) {
              this.push(message);
            }
            callback();
          } catch (err) {
            callback(err as Error);
          }
        },
      }),
      (err) => {
        if (err) {
          console.error(err);
        }
      }
    );
  }
}
