import { MessageRef } from "@streamr/protocol";

export const MIN_SEQUENCE_NUMBER_VALUE = 0;
export const MAX_SEQUENCE_NUMBER_VALUE = 2147483647;

export function minMessageRef(a: MessageRef | undefined, b: MessageRef | undefined) {
  if (!a && !b) {
    return undefined;
  }

  if (!a) {
    return b;
  }

  if (!b) {
    return a;
  }

  if (a.timestamp < b.timestamp) {
    return a;
  }

  if (b.timestamp < a.timestamp) {
    return b;
  }

  if ((a.sequenceNumber ?? MIN_SEQUENCE_NUMBER_VALUE) < (b.sequenceNumber ?? MIN_SEQUENCE_NUMBER_VALUE)) {
    return a;
  } else {
    return b;
  }
}

export function maxMessageRef(a: MessageRef | undefined, b: MessageRef | undefined) {
  if (!a && !b) {
    return undefined;
  }

  if (!a) {
    return b;
  }

  if (!b) {
    return a;
  }

  if (a.timestamp > b.timestamp) {
    return a;
  }

  if (b.timestamp > a.timestamp) {
    return b;
  }

  if ((a.sequenceNumber ?? MIN_SEQUENCE_NUMBER_VALUE) > (b.sequenceNumber ?? MIN_SEQUENCE_NUMBER_VALUE)) {
    return a;
  } else {
    return b;
  }
}

export function equalMessaRef(a: MessageRef, b: MessageRef) {
  return (
    a.timestamp === b.timestamp &&
    a.sequenceNumber === b.sequenceNumber
  );
}
