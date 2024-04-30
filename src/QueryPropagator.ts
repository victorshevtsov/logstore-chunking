import { MessageID, StreamMessage } from "@streamr/protocol";
import { PassThrough, pipeline } from "stream";
import { ChunkCallback, QueryChipper } from "./QueryChipper";
import { QueryParams, QueryRef } from "./QueryParams";
import { QueryState } from "./QueryState";
import { Storage } from "./Storage";
import { QueryResponse } from "./protocol/QueryResponse";

export class QueryPropagator {

  private readonly storage: Storage;
  private readonly queryParams: QueryParams;

  private readonly primaryNodeState: QueryState;
  private readonly foreignNodeState: QueryState;
  private readonly propagationStream: PassThrough;

  constructor(
    storage: Storage,
    queryParams: QueryParams,
    responseChunkCallback: ChunkCallback,
    propagationChunkCallback: ChunkCallback
  ) {
    this.storage = storage;
    this.queryParams = queryParams;

    this.primaryNodeState = new QueryState();
    this.foreignNodeState = new QueryState();
    this.propagationStream = new PassThrough({ objectMode: true });

    pipeline(
      this.storage.query(this.queryParams),
      // TODO: Try PassTrhough here instead of data event
      new QueryChipper(responseChunkCallback),
      (err) => {
        if (err) {
          // TODO: Handle error
          console.error(err);
        }
      }
    )
      .on("data", (messageStr: string) => {
        const message = StreamMessage.deserialize(messageStr);
        this.foreignNodeState.addResponseMessageId(message.messageId);
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

    response.messageIds.forEach(messageIdStr => {
      const messageIdJson = JSON.parse(messageIdStr);
      // @ts-expect-error Property 'fromArray' does not exist on type 'typeof MessageID'
      const messageId = MessageID.fromArray(messageIdJson);
      this.primaryNodeState.addResponseMessageId(messageId)
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
    const shrinkQueryRef = QueryRef.min(primaryNodeShrink, foreignNodeShrink);

    if (shrinkQueryRef) {
      const messaqgeIds = Array.from(this.foreignNodeState.subtract(this.primaryNodeState));
      if (messaqgeIds.length) {
        const queryStream = this.storage.queryByMessageIds(messaqgeIds);
        queryStream.pipe(this.propagationStream, { end: isFinalized });

        this.primaryNodeState.shrink(shrinkQueryRef);
        this.foreignNodeState.shrink(shrinkQueryRef);
      }
    } else if (isFinalized) {
      this.propagationStream.end();
    }

    // TODO: Handle an errror if the pipe gets broken
  }
}
