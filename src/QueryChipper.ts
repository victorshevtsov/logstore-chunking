import { PassThrough, TransformCallback } from "stream";

const LENGTH_LIMIT = 1000;
const SIZE_LIMIT = 1024 * 1024;

export type ChunkCallback = (chunk: any[], isFinal: boolean) => void;

interface ChunkOptions {
  lengthLimit?: number;
  sizeLimit?: number;
}

export class QueryChipper extends PassThrough {
  private readonly lengthLimit: number;
  private readonly sizeLimit: number;
  private readonly chunkCallback: ChunkCallback;
  private readonly chunk: any[] = [];
  private chunkSize: number = 0;

  constructor(
    chunkCallback: ChunkCallback,
    {
      lengthLimit = LENGTH_LIMIT,
      sizeLimit = SIZE_LIMIT,
    }: ChunkOptions = {}) {
    super({ objectMode: true });

    this.lengthLimit = lengthLimit;
    this.sizeLimit = sizeLimit;
    this.chunkCallback = chunkCallback;

    this.on('finish', () => {
      this.finishCunkIfReady(true);
    });
  }

  _transform(data: any, _encoding: BufferEncoding, callback: TransformCallback): void {
    this.chunk.push(data);
    this.chunkSize += (data as string).length;

    try {
      this.finishCunkIfReady(false)
    } catch (err) {
      callback(err as Error);
    }
    callback(null, data);
  }

  private finishCunkIfReady(isFinal: boolean) {
    if (
      isFinal ||
      this.chunk.length >= this.lengthLimit ||
      this.chunkSize >= this.sizeLimit) {
      this.chunkCallback(this.chunk.splice(0), isFinal);
      this.chunkSize = 0;
    }
  }
}
