import { convertBytesToStreamMessage } from "@streamr/trackerless-network";
import { EthereumAddress } from "@streamr/utils";
import { PassThrough, pipeline } from "stream";
import { minMessageRef } from "./MessageRef";
import { ChunkCallback, QueryChipper } from "./QueryChipper";
import { QueryParams } from "./QueryParams";
import { QueryState } from "./QueryState";
import { Storage } from "./Storage";
import { QueryPropagation } from "./protocol/QueryPropagation";
import { QueryResponse } from "./protocol/QueryResponse";

export class QueryAggregator extends PassThrough {

  private readonly storage: Storage;
  private readonly queryParams: QueryParams;

  private readonly primaryNodeState: QueryState;
  private readonly foreignNodeStates: Map<EthereumAddress, QueryState>;

  constructor(
    storage: Storage,
    queryParams: QueryParams,
    onlineNodes: EthereumAddress[],
    chunkCallback: ChunkCallback) {
    super({ objectMode: true });

    this.storage = storage;
    this.queryParams = queryParams;

    this.primaryNodeState = new QueryState();
    this.foreignNodeStates = new Map(
      onlineNodes.map(
        node => [node, new QueryState()]
      )
    );

    pipeline(
      this.storage.query(this.queryParams),
      new QueryChipper(chunkCallback),
      (err) => {
        // TODO: Handle error
      }
    )
      .on("data", (bytes: Uint8Array) => {
        const message = convertBytesToStreamMessage(bytes);
        this.primaryNodeState.addResponseMessageRef(message.messageId.toMessageRef());
        this.doCheck();
      })
      .on("end", () => {
        this.primaryNodeState.finalizeResponse();
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

  public onForeignResponse(node: EthereumAddress, response: QueryResponse) {
    const foreignNodeState = this.getOrCreateState(node);

    response.messageRefs.forEach(messageRef => {
      foreignNodeState.addResponseMessageRef(messageRef)
    });

    if (response.isFinal) {
      foreignNodeState.finalizeResponse();
    }

    this.doCheck();
  }

  public onPropagation(node: EthereumAddress, response: QueryPropagation) {
    const foreignNodeState = this.getOrCreateState(node);

    response.payload.forEach(async bytes => {
      const message = convertBytesToStreamMessage(bytes);
      await this.storage.store(message)

      foreignNodeState.addPropagationMessageRef(message.messageId.toMessageRef())
    });

    if (response.isFinal) {
      foreignNodeState.finalizePropagation();
    }

    this.doCheck();
  }

  private doCheck() {
    if (!this.primaryNodeState.isInitialized) {
      return;
    }

    let readyFrom = this.primaryNodeState.min;
    let readyTo = this.primaryNodeState.max;
    let isFinalized = this.primaryNodeState.isFinalizedResponse;

    for (const [, foreignNodeState] of this.foreignNodeStates) {
      if (!foreignNodeState.isInitialized) {
        return;
      }
      if (
        !foreignNodeState.isFinalizedResponse &&
        (!foreignNodeState.min || !foreignNodeState.max)
      ) {
        return;
      }

      readyFrom = minMessageRef(readyFrom, foreignNodeState.min);
      readyTo = minMessageRef(readyTo, foreignNodeState.max);
      isFinalized &&= foreignNodeState.isFinalizedResponse;
    }

    if (readyFrom && readyTo) {
      const queryParams: QueryParams = {
        ...this.queryParams,
        from: readyFrom,
        to: readyTo,
      };

      // TODO: Handle an errror if the pipe gets broken
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
