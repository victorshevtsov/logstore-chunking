import { MessageRef } from "@streamr/protocol";

export class QueryState {
  private _isInitialized: boolean = false;
  private _isFinalizedResponse: boolean = false;
  private _isFinalizedPropagation: boolean = false;
  private _lastPropagatedMessageRef: MessageRef | undefined;
  private readonly _messageRefs: MessageRef[] = [];

  public get isInitialized() {
    return this._isInitialized;
  }

  public get isFinalizedResponse() {
    return this._isFinalizedResponse;
  }

  public get isFinalizedPropagation() {
    return this._isFinalizedPropagation;
  }

  public get lastPropagatedMessageRef() {
    return this._lastPropagatedMessageRef;
  }

  public get min(): MessageRef | undefined {
    if (!this._messageRefs.length) {
      return undefined;
    }

    return new MessageRef(
      this._messageRefs[0].timestamp,
      this._messageRefs[0].sequenceNumber
    );
  }

  public get max(): MessageRef | undefined {
    if (!this._messageRefs.length) {
      return undefined;
    }

    return new MessageRef(
      this._messageRefs[this._messageRefs.length - 1].timestamp,
      this._messageRefs[this._messageRefs.length - 1].sequenceNumber);
  }

  public subtract(messageRefs: Iterable<MessageRef>) {
    const result = new Set<MessageRef>(this._messageRefs);

    for (const messageRef of messageRefs) {
      result.delete(messageRef);
    }

    return result.values();
  }

  *[Symbol.iterator]() {
    yield* this._messageRefs;
  }

  public addResponseMessageRef(messageRef: MessageRef) {
    this._isInitialized = true;
    this._messageRefs.push(messageRef);
  }

  public addPropagationMessageRef(messageRef: MessageRef) {
    this._isInitialized = true;
    this._lastPropagatedMessageRef = messageRef;
  }

  public finalizeResponse() {
    if (this._isFinalizedResponse) {
      throw new Error("Cannot finalize response, it is already finalized.")
    }
    this._isInitialized = true;
    this._isFinalizedResponse = true;
  }

  public finalizePropagation() {
    if (this._isFinalizedPropagation) {
      throw new Error("Cannot finalize propagation, it is already finalized.")
    }
    // this._isInitialized = true;
    this._isFinalizedPropagation = true;
  }

  public shrink(messageRef: MessageRef) {
    let count = 0;
    let index = 0;
    while (index < this._messageRefs.length) {
      const messageId = this._messageRefs[index];

      if (
        (messageId.timestamp < messageRef.timestamp) ||
        (
          messageId.timestamp === messageRef.timestamp &&
          messageId.sequenceNumber <= messageRef.sequenceNumber!
        )
      ) {
        count++;
      }

      if (messageId.timestamp > messageRef.timestamp) {
        break;
      }

      index++;
    }

    if (count > 0) {
      this._messageRefs.splice(0, count);
    }
  }
}
