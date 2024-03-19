import { MessageID } from "@streamr/protocol";
import { QueryRef } from "./QueryParams";

const CHUNK_LIMIT = 2;

export class QueryState {
  private _isInitialized: boolean = false;
  private _isFinalized: boolean = false;
  private readonly _messageIds: MessageID[] = [];
  // private chunkHead: number = 0;

  public get isInitialized() {
    return this._isInitialized;
  }

  public get isFinalized() {
    return this._isFinalized;
  }

  public get min(): QueryRef | undefined {
    if (!this._messageIds.length) {
      return undefined;
    }

    return {
      timestamp: this._messageIds[0].timestamp,
      sequenceNumber: this._messageIds[0].sequenceNumber,
    }
  }

  public get max(): QueryRef | undefined {
    if (!this._messageIds.length) {
      return undefined;
    }


    return {
      timestamp: this._messageIds[this._messageIds.length - 1].timestamp,
      sequenceNumber: this._messageIds[this._messageIds.length - 1].sequenceNumber,
    }
  }

  public addMessageId(messageId: MessageID) {
    this._isInitialized = true;
    this._messageIds.push(messageId);
  }

  public finalize() {
    //   // TODO: Check?
    this._isFinalized = true;
  }

  // public get isChunkReady() {
  //   return this._messageIds.length - this.chunkHead >= CHUNK_LIMIT ||
  //     this._isFinalResponseReceived;
  // }

  // public finishChunk() {
  //   const head = this.chunkHead;
  //   this.chunkHead = this._messageIds.length - 1;
  //   return this._messageIds.slice(head);
  // }

  public shrink(queryRef: QueryRef) {
    let count = 0;
    let index = 0;
    while (index < this._messageIds.length) {
      const messageId = this._messageIds[index];

      if (
        (messageId.timestamp < queryRef.timestamp) ||
        (
          messageId.timestamp === queryRef.timestamp &&
          messageId.sequenceNumber <= queryRef.sequenceNumber!
        )
      ) {
        count++;
      }

      if (messageId.timestamp > queryRef.timestamp) {
        break;
      }

      index++;
    }

    if (count > 0) {
      this._messageIds.splice(0, count);

      // if (this._messageIds.length > 0) {
      //   this._minQueryRef = {
      //     timestamp: this._messageIds[0].timestamp,
      //     sequenceNumber: this._messageIds[0].sequenceNumber,
      //   }
      // } else {
      //   this._minQueryRef = undefined;
      //   this._maxQueryRef = undefined;
      // }
    }
  }
}
