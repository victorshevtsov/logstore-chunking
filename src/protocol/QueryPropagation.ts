export class QueryPropagation {
  public readonly requestId: string;
  public readonly isFinal: boolean;
  public readonly payload: string[];

  constructor(requestId: string, payload: string[], isFinal: boolean) {
    this.requestId = requestId;
    this.isFinal = isFinal;
    this.payload = payload;
  }
}
