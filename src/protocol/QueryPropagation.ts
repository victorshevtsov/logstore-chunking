export class QueryPropagation {
  public readonly requestId: string;
  public readonly isFinal: boolean;
  public readonly payload: Uint8Array[];

  constructor(requestId: string, payload: Uint8Array[], isFinal: boolean) {
    this.requestId = requestId;
    this.isFinal = isFinal;
    this.payload = payload;
  }
}
