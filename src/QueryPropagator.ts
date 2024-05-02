import { MessageID, toStreamID } from "@streamr/protocol";
import { convertBytesToStreamMessage } from "@streamr/trackerless-network";
import { toEthereumAddress } from "@streamr/utils";
import { PassThrough, pipeline } from "stream";
import { MSG_CHAIN_ID, PUBLISHER_ID } from "../test/test-utils";
import { minMessageRef } from "./MessageRef";
import { ChunkCallback, QueryChipper } from "./QueryChipper";
import { QueryState } from "./QueryState";
import { Storage } from "./Storage";
import { QueryRangeOptions, QueryRequest } from "./protocol/QueryRequest";
import { QueryResponse } from "./protocol/QueryResponse";

export class QueryPropagator {

  private readonly storage: Storage;
  private readonly queryRequest: QueryRequest;

  private readonly primaryNodeState: QueryState;
  private readonly foreignNodeState: QueryState;
  private readonly propagationStream: PassThrough;

  constructor(
    storage: Storage,
    queryRequest: QueryRequest,
    responseChunkCallback: ChunkCallback,
    propagationChunkCallback: ChunkCallback
  ) {
    this.storage = storage;
    this.queryRequest = queryRequest;

    this.primaryNodeState = new QueryState();
    this.foreignNodeState = new QueryState();
    this.propagationStream = new PassThrough({ objectMode: true });

    // TODO: review the cast
    const queryRangeOptions = this.queryRequest.queryOptions as QueryRangeOptions;

    pipeline(
      this.storage.query(
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
        this.foreignNodeState.addResponseMessageRef(message.messageId.toMessageRef());
        this.doCheck();
      })
      .on("end", () => {
        this.foreignNodeState.finalizeResponse();
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
      this.primaryNodeState.addResponseMessageRef(messageRef)
    });

    if (response.isFinal) {
      this.primaryNodeState.finalizeResponse();
    }

    this.doCheck();
  }

  private doCheck() {
    let isFinalized = this.primaryNodeState.isFinalizedResponse && this.foreignNodeState.isFinalizedResponse;

    if (
      !this.primaryNodeState.isFinalizedResponse && !this.primaryNodeState.max ||
      !this.foreignNodeState.isFinalizedResponse && !this.foreignNodeState.max
    ) {
      return;
    }

    const primaryNodeShrink = this.primaryNodeState.isFinalizedResponse ? undefined : this.primaryNodeState.max;
    const foreignNodeShrink = this.foreignNodeState.isFinalizedResponse ? undefined : this.foreignNodeState.max;
    const shrinkMessageRef = minMessageRef(primaryNodeShrink, foreignNodeShrink);

    if (shrinkMessageRef) {
      const messaqgeRefs = Array.from(this.foreignNodeState.subtract(this.primaryNodeState));
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

        this.primaryNodeState.shrink(shrinkMessageRef);
        this.foreignNodeState.shrink(shrinkMessageRef);
      }
    } else if (isFinalized) {
      this.propagationStream.end();
    }

    // TODO: Handle an errror if the pipe gets broken
  }
}
