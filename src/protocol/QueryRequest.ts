import { MessageRef } from "@streamr/protocol";
import { SystemMessage, SystemMessageType } from "./SystemMessage";

export enum QueryType {
  Last = 'last',
  From = 'from',
  Range = 'range',
}

/**
 * Query the latest "n" messages.
 */
export interface QueryLastOptions {
  queryType: QueryType.Last;
  last: number;
}

/**
 * Query messages starting from a given point in time.
 */
export interface QueryFromOptions {
  queryType: QueryType.From;
  from: MessageRef;
  publisherId?: string;
  limit?: number;
}

/**
 * Query messages between two points in time.
 */
export interface QueryRangeOptions {
  queryType: QueryType.Range;
  from: MessageRef;
  to: MessageRef;
  msgChainId?: string;
  publisherId?: string;
  limit?: number;
}

/**
 * The supported Query types.
 */
export type QueryOptions =
  | QueryLastOptions
  | QueryFromOptions
  | QueryRangeOptions;

interface QueryRequestOptions {
  requestId: string;
  consumerId: string;
  streamId: string;
  partition: number;
  queryOptions: QueryOptions;
}

export class QueryRequest extends SystemMessage {
  requestId: string;
  consumerId: string;
  streamId: string;
  partition: number;
  queryOptions: QueryOptions;

  constructor({
    requestId,
    consumerId,
    streamId,
    partition,
    queryOptions,
  }: QueryRequestOptions) {
    super(SystemMessageType.QueryRequest);

    // TODO: Validate the arguments
    this.requestId = requestId;
    this.consumerId = consumerId;
    this.streamId = streamId;
    this.partition = partition;
    this.queryOptions = queryOptions;
  }
}
