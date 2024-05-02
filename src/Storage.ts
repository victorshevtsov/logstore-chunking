import { MessageID, StreamMessage } from '@streamr/protocol';
import { convertBytesToStreamMessage, convertStreamMessageToBytes } from '@streamr/trackerless-network';
import { Readable, Transform, pipeline } from "stream";
import { MAX_SEQUENCE_NUMBER_VALUE, MIN_SEQUENCE_NUMBER_VALUE } from './MessageRef';

export class Storage {
  private readonly data: Uint8Array[];

  constructor(data: Uint8Array[] = []) {
    this.data = data;
  }

  public async store(streamMessage: StreamMessage) {
    this.data.push(convertStreamMessageToBytes(streamMessage));

    this.data.sort((a: Uint8Array, b: Uint8Array) => {
      const msgA = convertBytesToStreamMessage(a);
      const msgB = convertBytesToStreamMessage(b);

      const msgRefA = msgA.messageId.toMessageRef();
      const msgRefB = msgB.messageId.toMessageRef();

      return msgRefA.compareTo(msgRefB);
    });
  }

  public query(
    streamId: string,
    partition: number,
    fromTimestamp: number,
    fromSequenceNo: number,
    toTimestamp: number,
    toSequenceNo: number,
    publisherId?: string,
    msgChainId?: string,
    limit?: number
  ): Readable {
    return pipeline(
      Readable.from(this.data),
      new Transform({
        objectMode: true,
        transform(chunk, encoding, callback) {
          try {
            const message = convertBytesToStreamMessage(chunk);
            const messageId = message.messageId;

            if (
              messageId.streamId === streamId &&
              messageId.streamPartition === partition &&
              (!publisherId || messageId.publisherId === publisherId) &&
              (!msgChainId || messageId.msgChainId === msgChainId)
            ) {
              if (messageId.streamId === streamId &&
                (messageId.timestamp > fromTimestamp ||
                  messageId.timestamp === fromTimestamp &&
                  messageId.sequenceNumber >= (fromSequenceNo ?? MIN_SEQUENCE_NUMBER_VALUE)) &&
                (messageId.timestamp < toTimestamp ||
                  messageId.timestamp === toTimestamp &&
                  messageId.sequenceNumber <= (toSequenceNo ?? MAX_SEQUENCE_NUMBER_VALUE)
                )) {
                this.push(chunk);
              }
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
