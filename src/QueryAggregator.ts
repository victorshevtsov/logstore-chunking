import { convertBytesToStreamMessage } from "@streamr/trackerless-network";
import { EthereumAddress } from "@streamr/utils";
import { PassThrough, pipeline } from "stream";
import { minMessageRef } from "./MessageRef";
import { MessageRefs } from "./MessageRefs";
import { ChunkCallback, QueryChipper } from "./QueryChipper";
import { Storage } from "./Storage";
import { QueryPropagation } from "./protocol/QueryPropagation";
import { QueryRangeOptions, QueryRequest } from "./protocol/QueryRequest";
import { QueryResponse } from "./protocol/QueryResponse";

export class QueryAggregator extends PassThrough {

  private readonly storage: Storage;
  private readonly queryRequest: QueryRequest;

  private readonly primaryNodeMessageRefs: MessageRefs;
  private readonly foreignNodeMessageRefs: Map<EthereumAddress, { responded: MessageRefs, propagated: MessageRefs }>;

  constructor(
    storage: Storage,
    queryRequest: QueryRequest,
    onlineNodes: EthereumAddress[],
    chunkCallback: ChunkCallback) {
    super({ objectMode: true });

    this.storage = storage;
    this.queryRequest = queryRequest;

    this.primaryNodeMessageRefs = new MessageRefs();
    this.foreignNodeMessageRefs = new Map<EthereumAddress, { responded: MessageRefs, propagated: MessageRefs }>(
      onlineNodes.map(
        node => [node, { responded: new MessageRefs(), propagated: new MessageRefs() }]
      )
    );

    // TODO: review the cast
    const queryRangeOptions = this.queryRequest.queryOptions as QueryRangeOptions;

    pipeline(
      this.storage.queryRange(
        this.queryRequest.streamId,
        this.queryRequest.partition,
        queryRangeOptions.from.timestamp,
        queryRangeOptions.from.sequenceNumber,
        queryRangeOptions.to.timestamp,
        queryRangeOptions.to.sequenceNumber,
        queryRangeOptions.publisherId,
        queryRangeOptions.msgChainId
      ),
      new QueryChipper(chunkCallback),
      (err) => {
        // TODO: Handle error
      }
    )
      .on("data", (bytes: Uint8Array) => {
        const message = convertBytesToStreamMessage(bytes);
        const messageRef = message.messageId.toMessageRef()
        this.primaryNodeMessageRefs.push(messageRef);
        this.doCheck();
      })
      .on("end", () => {
        this.primaryNodeMessageRefs.finalize();
        this.doCheck();
      });
  }

  private getOrCreateForeignNodeMeesageRefs(node: EthereumAddress) {
    let messageRefs = this.foreignNodeMessageRefs.get(node);

    if (!messageRefs) {
      messageRefs = { responded: new MessageRefs(), propagated: new MessageRefs };
      this.foreignNodeMessageRefs.set(node, messageRefs);
    }

    return messageRefs;
  }

  public onForeignResponse(node: EthereumAddress, response: QueryResponse) {
    const foreignNodeMessageRefs = this.getOrCreateForeignNodeMeesageRefs(node);

    response.messageRefs.forEach(messageRef => {
      foreignNodeMessageRefs.responded.push(messageRef);
    });

    if (response.isFinal) {
      foreignNodeMessageRefs.responded.finalize();
    }

    this.doCheck();
  }

  public onPropagation(node: EthereumAddress, response: QueryPropagation) {
    const foreignNodeMessageRefs = this.getOrCreateForeignNodeMeesageRefs(node);

    Promise
      .all(
        response.payload.map(async (bytes: Uint8Array) => {
          const message = convertBytesToStreamMessage(bytes);
          await this.storage.store(message);

          const messageRef = message.messageId.toMessageRef();
          foreignNodeMessageRefs.propagated.push(messageRef)
        }))
      .then(() => {
        if (response.isFinal) {
          foreignNodeMessageRefs.propagated.finalize();
        }

        this.doCheck();
      });
  }

  private doCheck() {
    if (!this.primaryNodeMessageRefs.isInitialized) {
      return;
    }

    let readyFrom = this.primaryNodeMessageRefs.min;
    let readyTo = this.primaryNodeMessageRefs.max;
    let isFinalized = this.primaryNodeMessageRefs.isFinalized;

    for (const [, foreignNodeMessageRefs] of this.foreignNodeMessageRefs) {
      if (!foreignNodeMessageRefs.responded.isInitialized) {
        return;
      }
      if (
        !foreignNodeMessageRefs.responded.isFinalized &&
        (!foreignNodeMessageRefs.responded.min || !foreignNodeMessageRefs.responded.max)
      ) {
        return;
      }

      readyFrom = minMessageRef(readyFrom, foreignNodeMessageRefs.responded.min);
      readyTo = minMessageRef(readyTo, foreignNodeMessageRefs.responded.max);
      isFinalized &&= foreignNodeMessageRefs.responded.isFinalized;
    }

    if (readyFrom && readyTo) {

      // TODO: review the cast
      const queryRangeOptions = this.queryRequest.queryOptions as QueryRangeOptions;

      // TODO: Handle an errror if the pipe gets broken
      const queryStream = this.storage.queryRange(
        this.queryRequest.streamId,
        this.queryRequest.partition,
        readyFrom.timestamp,
        readyFrom.sequenceNumber,
        readyTo.timestamp,
        readyTo.sequenceNumber,
        queryRangeOptions.publisherId,
        queryRangeOptions.msgChainId
      );

      queryStream.pipe(this, { end: isFinalized });

      this.primaryNodeMessageRefs.shrink(readyTo);
      for (const [, foreignNodeMessageRefs] of this.foreignNodeMessageRefs) {
        foreignNodeMessageRefs.responded.shrink(readyTo);
        foreignNodeMessageRefs.propagated.shrink(readyTo);
      }
    } else if (isFinalized) {
      this.end();
    }
  }
}
