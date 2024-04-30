import { StreamMessage } from "@streamr/protocol";
import { QueryPropagation } from "../src/protocol/QueryPropagation";
import { QueryResponse } from "../src/protocol/QueryResponse";

export function createQueryResponse(requestId: string, serializedMessages: string[], isFinal: boolean) {
  const messageIds = serializedMessages.map(serializedMessage =>
    StreamMessage.deserialize(serializedMessage).messageId.serialize()
  );

  return new QueryResponse(requestId, messageIds, isFinal);
}

export function createQueryPropagation(requestId: string, serializedMessages: string[], isFinal: boolean) {
  return new QueryPropagation(requestId, serializedMessages, isFinal);
}
