import { MessageRef } from "@streamr/protocol";

export class QueryResponse {
  public readonly requestId: string;
  public readonly isFinal: boolean;
  public readonly messageRefs: MessageRef[] = [];

  constructor(requestId: string, messageRefs: MessageRef[], isFinal: boolean) {
    this.requestId = requestId;
    this.isFinal = isFinal;
    this.messageRefs = messageRefs;
  }
}
