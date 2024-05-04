import { MessageID, toStreamID } from "@streamr/protocol";
import { convertBytesToStreamMessage } from "@streamr/trackerless-network";
import { toEthereumAddress } from "@streamr/utils";
import { PassThrough, pipeline } from "stream";
import { MSG_CHAIN_ID, PUBLISHER_ID } from "../test/test-utils";
import { minMessageRef } from "./MessageRef";
import { MessageRefs } from "./MessageRefs";
import { ChunkCallback, QueryChipper } from "./QueryChipper";
import { Storage } from "./Storage";
import { QueryRangeOptions, QueryRequest } from "./protocol/QueryRequest";
import { QueryResponse } from "./protocol/QueryResponse";

export class QueryPropagator {

  private readonly storage: Storage;
  private readonly queryRequest: QueryRequest;

  private readonly primaryNodeMessageRefs: MessageRefs;
  private readonly foreignNodeMessageRefs: MessageRefs;
  private readonly propagationStream: PassThrough;

  constructor(
    storage: Storage,
    queryRequest: QueryRequest,
    responseChunkCallback: ChunkCallback,
    propagationChunkCallback: ChunkCallback
  ) {
    this.storage = storage;
    this.queryRequest = queryRequest;

    this.primaryNodeMessageRefs = new MessageRefs();
    this.foreignNodeMessageRefs = new MessageRefs();
    this.propagationStream = new PassThrough({ objectMode: true });

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
      // TODO: Try PassTrhough here instead of data event
      new QueryChipper(responseChunkCallback),
      (err) => {
        if (err) {
          // TODO: Handle error
          console.error(err);
        }
      }
    )
      .on("data", (bytes: Uint8Array) => {
        const message = convertBytesToStreamMessage(bytes);
        this.foreignNodeMessageRefs.push(message.messageId.toMessageRef());
        this.doCheck();
      })
      .on("end", () => {
        this.foreignNodeMessageRefs.finalize();
        this.doCheck();
      });

    pipeline(
      this.propagationStream,
      new QueryChipper(propagationChunkCallback),
      (err) => {
        if (err) {
          // TODO: Handle error
          console.error(err);
        }
      }
    );
  }

  public onPrimaryResponse(response: QueryResponse) {

    response.messageRefs.forEach(messageRef => {
      this.primaryNodeMessageRefs.push(messageRef)
    });

    if (response.isFinal) {
      this.primaryNodeMessageRefs.finalize();
    }

    this.doCheck();
  }

  private doCheck() {
    let isFinalized =
      this.primaryNodeMessageRefs.isFinalized &&
      this.foreignNodeMessageRefs.isFinalized

    if (
      (!this.primaryNodeMessageRefs.isFinalized && !this.primaryNodeMessageRefs.max) ||
      (!this.foreignNodeMessageRefs.isFinalized && !this.foreignNodeMessageRefs.max)
    ) {
      return;
    }

    const primaryNodeShrink =
      this.primaryNodeMessageRefs.isFinalized
        ? undefined
        : this.primaryNodeMessageRefs.max;

    const foreignNodeShrink =
      this.foreignNodeMessageRefs.isFinalized
        ? undefined
        : this.foreignNodeMessageRefs.max;

    const shrinkMessageRef = minMessageRef(primaryNodeShrink, foreignNodeShrink);

    if (shrinkMessageRef) {
      const messaqgeRefs = Array.from(this.foreignNodeMessageRefs.subtract(this.primaryNodeMessageRefs));
      if (messaqgeRefs.length) {
        const queryStream = this.storage.queryByMessageIds(
          messaqgeRefs.map(messageRef => new MessageID(
            toStreamID(this.queryRequest.streamId),
            this.queryRequest.partition,
            messageRef.timestamp,
            messageRef.sequenceNumber,
            toEthereumAddress(PUBLISHER_ID), // TODO: publisherId
            MSG_CHAIN_ID, // TODO: msgChainId
          )));
        queryStream.pipe(this.propagationStream, { end: isFinalized });

        this.primaryNodeMessageRefs.shrink(shrinkMessageRef);
        this.foreignNodeMessageRefs.shrink(shrinkMessageRef);
      }
    } else if (isFinalized) {
      this.propagationStream.end();
    }

    // TODO: Handle an errror if the pipe gets broken
  }
}
