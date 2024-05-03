import { MessageRef } from "@streamr/protocol";
import { maxMessageRef, minMessageRef } from "./MessageRef";

export class MessageRefs {
  private _isInitialized: boolean = false;
  private _isFinalized: boolean = false;
  private _items: MessageRef[] = [];
  private _threshold: MessageRef | undefined;

  *[Symbol.iterator]() {
    yield* this._items;
  }

  public get isInitialized(): boolean {
    return this._isInitialized;
  }

  public get isFinalized(): boolean {
    return this._isFinalized;
  }

  public finalize() {
    if (this._isFinalized) {
      throw new Error("Cannot finalize an already finalized") // TODO:
    }

    this._isInitialized = true;
    this._isFinalized = true;
  }

  public get min(): MessageRef | undefined {
    return this._items.reduce((prev, ref) => {
      return minMessageRef(prev, ref);
    }, undefined as MessageRef | undefined);
  }

  public get max(): MessageRef | undefined {
    return this._items.reduce((prev, ref) => {
      return maxMessageRef(prev, ref);
    }, undefined as MessageRef | undefined);
  }

  public push(item: MessageRef) {
    if (this._isFinalized) {
      throw new Error("Cannot push to a finalized"); // TODO:
    }

    if (this._threshold && this._threshold.compareTo(item) >= 1) {
      return;
    }

    this._items.push(item);
    this._isInitialized = true;
  }

  public shrink(threshold: MessageRef) {
    this._threshold = threshold;

    let count = 0;
    let index = 0;
    while (index < this._items.length) {
      const ref = this._items[index];

      if (
        (ref.timestamp < threshold.timestamp) ||
        (
          ref.timestamp === threshold.timestamp &&
          ref.sequenceNumber <= threshold.sequenceNumber!
        )
      ) {
        count++;
      }

      if (ref.timestamp > threshold.timestamp) {
        break;
      }

      index++;
    }

    if (count > 0) {
      this._items.splice(0, count);
    }
  }

  public subtract(messageRefs: Iterable<MessageRef>) {
    const result = new Set<MessageRef>(this._items);

    for (const messageRef of messageRefs) {
      result.delete(messageRef);
    }

    return result.values();
  }
}
