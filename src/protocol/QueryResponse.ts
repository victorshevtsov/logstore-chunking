import { MessageRef } from "@streamr/protocol";
import { SystemMessage, SystemMessageType } from "./SystemMessage";

export class QueryResponse extends SystemMessage {
  public readonly requestId: string;
  public readonly isFinal: boolean;
  public readonly messageRefs: MessageRef[] = [];

  constructor(requestId: string, messageRefs: MessageRef[], isFinal: boolean) {
    super(SystemMessageType.QueryResponse);

    this.requestId = requestId;
    this.isFinal = isFinal;
    this.messageRefs = messageRefs;
  }
}
