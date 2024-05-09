import { MessageID, MessageRef, StreamMessage } from '@streamr/protocol';
import { convertBytesToStreamMessage, convertStreamMessageToBytes } from '@streamr/trackerless-network';
import { Readable, Transform, pipeline } from "stream";
import { MAX_SEQUENCE_NUMBER_VALUE, MIN_SEQUENCE_NUMBER_VALUE } from './LogStore';

export class DatabaseAdapter {
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

  public queryRange(
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
        transform(chunk, _encoding, callback) {
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
        transform(chunk, _encoding, callback) {
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

  public queryByMessageRefs(
    streamId: string,
    partition: number,
    messageRefs: MessageRef[]
  ): Readable {
    return pipeline(
      Readable.from(this.data),
      new Transform({
        objectMode: true,
        transform(chunk, _encoding, callback) {
          try {
            const message = convertBytesToStreamMessage(chunk);
            const messageId = message.messageId;
            const messageRef = messageId.toMessageRef();

            if (
              messageId.streamId === streamId &&
              messageId.streamPartition === partition &&
              messageRefs.find(ref => ref.compareTo(messageRef))
            ) {
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

  public queryLast(
    streamId: string,
    partition: number,
    requestCount: number
  ): Readable {
    let count = 0;
    return pipeline(
      Readable.from([...this.data].reverse()),
      new Transform({
        objectMode: true,
        transform(chunk, _encoding, callback) {
          if (count < requestCount) { // TODO: review the logic
            try {
              const message = convertBytesToStreamMessage(chunk);
              const messageId = message.messageId;

              if (
                messageId.streamId === streamId &&
                messageId.streamPartition === partition
              ) {
                this.push(chunk);
                count++;
              }
              callback();
            } catch (err) {
              callback(err as Error);
            }
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
