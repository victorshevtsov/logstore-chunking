import { MessageID } from "@streamr/protocol";
import { QueryRef } from "./QueryParams";

export class QueryState {
  private _isInitialized: boolean = false;
  private _isFinalizedResponse: boolean = false;
  private _isFinalizedPropagation: boolean = false;
  private _lastPropagatedMessageId: MessageID | undefined;
  private readonly _messageIds: MessageID[] = [];

  public get isInitialized() {
    return this._isInitialized;
  }

  public get isFinalizedResponse() {
    return this._isFinalizedResponse;
  }

  public get isFinalizedPropagation() {
    return this._isFinalizedPropagation;
  }

  public get lastPropagatedMessageId() {
    return this._lastPropagatedMessageId;
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

  public subtract(messageIds: Iterable<MessageID>) {
    const result = new Map<string, MessageID>(this._messageIds.map(m => [m.serialize(), m]));

    for (const messageId of messageIds) {
      result.delete(messageId.serialize());
    }

    return result.values();
  }

  *[Symbol.iterator]() {
    yield* this._messageIds;
  }

  public addResponseMessageId(messageId: MessageID) {
    this._isInitialized = true;
    this._messageIds.push(messageId);
  }

  public addPropagationMessageId(messageId: MessageID) {
    this._isInitialized = true;
    this._lastPropagatedMessageId = messageId;
  }

  public finalizeResponse() {
    if (this._isFinalizedResponse) {
      throw new Error("Cannot finalize response, it isalready finalized.")
    }
    this._isInitialized = true;
    this._isFinalizedResponse = true;
  }

  public finalizePropagation() {
    if (this._isFinalizedPropagation) {
      throw new Error("Cannot finalize propagation, is is already finalized.")
    }
    // this._isInitialized = true;
    this._isFinalizedPropagation = true;
  }

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
    }
  }
}
