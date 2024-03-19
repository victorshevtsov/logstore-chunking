import { StreamMessage } from '@streamr/protocol';
import { Readable, Transform, pipeline } from "stream";
import { MAX_SEQUENCE_NUMBER_VALUE, MIN_SEQUENCE_NUMBER_VALUE, QueryParams } from "./QueryParams";

export class Storage {
  private readonly data: string[];

  constructor(data: string[]) {
    this.data = data;
  }

  public query(params: QueryParams): Readable {
    return pipeline(
      Readable.from(this.data),
      new Transform({
        objectMode: true,
        transform(chunk, encoding, callback) {
          try {
            const chunkStr = chunk.toString()
            const chunkJSON = JSON.parse(chunkStr);
            const message = StreamMessage.deserialize(chunkJSON);
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
}
