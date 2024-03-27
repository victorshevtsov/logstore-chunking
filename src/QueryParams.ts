export const MIN_SEQUENCE_NUMBER_VALUE = 0;
export const MAX_SEQUENCE_NUMBER_VALUE = 2147483647;

export interface QueryRef {
  timestamp: number;
  sequenceNumber?: number;
};

export class QueryRef {
  static min(a: QueryRef | undefined, b: QueryRef | undefined) {
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

  static max(a: QueryRef | undefined, b: QueryRef | undefined) {
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

  static equals(a: QueryRef, b: QueryRef) {
    return (
      a.timestamp === b.timestamp &&
      a.sequenceNumber === b.sequenceNumber
    );
  }
}

export interface QueryParams {
  streamId: string;
  from: QueryRef;
  to: QueryRef;
}
