import { EthereumAddress } from "@streamr/utils";
import { Writable, pipeline } from "stream";
import { QueryAggregator } from "./QueryAggregator";
import { ChunkCallback, QueryChipper } from "./QueryChipper";
import { QueryParams } from "./QueryParams";
import { Storage } from "./Storage";
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
    params: QueryParams,
    resposeStream: Writable,
    onlineNodes: EthereumAddress[],
    chunkCallback: ChunkCallback) {
    const queryStream = this.storage.query(params);
    const queryChipper = new QueryChipper(chunkCallback);
    const queryAggregator = new QueryAggregator(this.storage, params, onlineNodes, chunkCallback);
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
