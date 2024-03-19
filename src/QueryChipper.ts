import { MessageID, StreamMessage } from "@streamr/protocol";
import { PassThrough, TransformCallback } from "stream";

const CHUNK_SIZE = 2;

export type ChunkCallback = (chunk: MessageID[], isFinal: boolean) => void;

export class QueryChipper extends PassThrough {
  private readonly chunkCallback: ChunkCallback;
  private readonly messsageIds: MessageID[] = [];

  constructor(chunkCallback: ChunkCallback) {
    super({ objectMode: true });

    this.chunkCallback = chunkCallback;
    this.on('finish', () => {
      this.finishCunkIfReady(true);
    });
  }

  _transform(chunk: any, encoding: BufferEncoding, callback: TransformCallback): void {
    const message = chunk as StreamMessage;
    this.messsageIds.push(message.messageId);

    try {
      this.finishCunkIfReady(false)
    } catch (err) {
      callback(err as Error);
    }
    callback(null, chunk);
  }

  private finishCunkIfReady(isFinal: boolean) {
    if (this.messsageIds.length >= CHUNK_SIZE || isFinal) {
      this.chunkCallback(this.messsageIds.splice(0), isFinal);
    }
  }
}
