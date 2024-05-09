import { SystemMessage, SystemMessageType } from "./SystemMessage";

export class QueryPropagate extends SystemMessage {
  public readonly requestId: string;
  public readonly payload: Uint8Array[];

  constructor(requestId: string, payload: Uint8Array[]) {
    super(SystemMessageType.QueryPropagate);

    this.requestId = requestId;
    this.payload = payload;
  }
}
