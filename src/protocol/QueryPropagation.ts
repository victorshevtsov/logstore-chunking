import { SystemMessage, SystemMessageType } from "./SystemMessage";

export class QueryPropagation extends SystemMessage {
  public readonly requestId: string;
  public readonly isFinal: boolean;
  public readonly payload: Uint8Array[];

  constructor(requestId: string, payload: Uint8Array[], isFinal: boolean) {
    super(SystemMessageType.QueryPropagation);

    this.requestId = requestId;
    this.isFinal = isFinal;
    this.payload = payload;
  }
}
