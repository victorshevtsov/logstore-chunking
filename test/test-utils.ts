import { StreamMessage } from "@streamr/protocol";
import { QueryResponse } from "../src/QueryResponse";

export function createQueryResponse(requestId: string, records: string[], isFinal: boolean) {
  const queryResponse = new QueryResponse(requestId, isFinal);

  records.forEach(messageStr => {
    const messageId = StreamMessage.deserialize(messageStr).messageId;
    queryResponse.messageIds.push(messageId.serialize());
  })

  return queryResponse;
}
