import { EthereumAddress } from "@streamr/utils";
import { Writable, pipeline } from "stream";
import { QueryAggregator } from "./QueryAggregator";
import { ChunkCallback, QueryChipper } from "./QueryChipper";
import { Storage } from "./Storage";
import { QueryRangeOptions, QueryRequest } from "./protocol/QueryRequest";
import { QueryResponse } from "./protocol/QueryResponse";

export class QueryResolver {
  private storage: Storage;

  private queryAggreagators: Map<string, QueryAggregator>;

  constructor(storage: Storage) {
    this.storage = storage;

    this.queryAggreagators = new Map();
  }

  public resolve(
    requestId: string,
    queryRequest: QueryRequest,
    resposeStream: Writable,
    onlineNodes: EthereumAddress[],
    chunkCallback: ChunkCallback) {
    // TODO: review the cast
    const queryRangeOptions = queryRequest.queryOptions as QueryRangeOptions;

    const queryStream = this.storage.query(
      queryRequest.streamId,
      queryRequest.partition,
      queryRangeOptions.from.timestamp,
      queryRangeOptions.from.sequenceNumber,
      queryRangeOptions.to.timestamp,
      queryRangeOptions.to.sequenceNumber,
      queryRangeOptions.publisherId,
      queryRangeOptions.msgChainId
    );
    const queryChipper = new QueryChipper(chunkCallback);
    const queryAggregator = new QueryAggregator(this.storage, queryRequest, onlineNodes, chunkCallback);
    this.queryAggreagators.set(requestId, queryAggregator);

    pipeline(queryStream, queryChipper, queryAggregator, resposeStream, (err) => {
      if (err) {
        console.error(err);
      }
    });
  }

  public onResponse(node: EthereumAddress, queryResponse: QueryResponse) {
    const resolution = this.queryAggreagators.get(queryResponse.requestId);
    resolution?.onForeignResponse(node, queryResponse);
  }
}
