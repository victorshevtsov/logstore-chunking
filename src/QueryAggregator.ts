import { MessageID, StreamMessage } from "@streamr/protocol";
import { EthereumAddress } from "@streamr/utils";
import { PassThrough } from "stream";
import { QueryState } from "./QeryState";
import { QueryParams, QueryRef } from "./QueryParams";
import { QueryResponse } from "./QueryResponse";
import { Storage } from "./Storage";

export class QueryAggregator extends PassThrough {

  private readonly storage: Storage;
  private readonly queryParams: QueryParams;

  private readonly primaryNodeState: QueryState;
  private readonly foreignNodeStates: Map<EthereumAddress, QueryState>;

  constructor(
    storage: Storage,
    queryParams: QueryParams,
    onlineNodes: EthereumAddress[]) {
    super({ objectMode: true });

    this.storage = storage;
    this.queryParams = queryParams;

    this.primaryNodeState = new QueryState();
    this.foreignNodeStates = new Map(
      onlineNodes.map(
        node => [node, new QueryState()]
      )
    );

    // TODO: Need a backpressure
    const queryStream = this.storage.query(queryParams);
    queryStream.on("data", (message: StreamMessage) => {
      this.primaryNodeState.addMessageId(message.messageId);
      this.doCheck();
    });
    queryStream.on("end", () => {
      this.primaryNodeState.finalize();
      this.doCheck();
    });
  }

  private getOrCreateState(node: EthereumAddress) {
    let state = this.foreignNodeStates.get(node);

    if (!state) {
      state = new QueryState();
      this.foreignNodeStates.set(node, state);
    }

    return state;
  }

  public onResponse(node: EthereumAddress, response: QueryResponse) {
    const nodeQueryState = this.getOrCreateState(node);

    response.messageIds.forEach(messageIdStr => {
      const messageIdJson = JSON.parse(messageIdStr);
      // @ts-expect-error Property 'fromArray' does not exist on type 'typeof MessageID'
      const messageId = MessageID.fromArray(messageIdJson);
      nodeQueryState.addMessageId(messageId)
    });

    if (response.isFinal) {
      nodeQueryState.finalize();
    }

    this.doCheck();
  }

  private doCheck() {
    if (!this.primaryNodeState.isInitialized) {
      return;
    }

    let readyFrom = this.primaryNodeState.min;
    let readyTo = this.primaryNodeState.max;
    let isFinalized = this.primaryNodeState.isFinalized;

    for (const [, state] of this.foreignNodeStates) {
      if (!state.isInitialized) {
        return;
      }
      if (!state.isFinalized && (!state.min || !state.max)) {
        return;
      }

      readyFrom = QueryRef.min(readyFrom, state.min);
      readyTo = QueryRef.min(readyTo, state.max);
      isFinalized &&= state.isFinalized;
    }

    if (readyFrom && readyTo) {
      const queryParams: QueryParams = {
        ...this.queryParams,
        from: readyFrom,
        to: readyTo,
      };

      const queryStream = this.storage.query(queryParams);
      queryStream.pipe(this, { end: isFinalized });

      this.primaryNodeState.shrink(readyTo);
      for (const [, foreignNodeState] of this.foreignNodeStates) {
        foreignNodeState.shrink(readyTo);
      }
    } else if (isFinalized) {
      this.end();
    }
  }
}
