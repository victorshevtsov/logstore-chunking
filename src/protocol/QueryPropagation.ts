export class QueryPropagation {
  public readonly requestId: string;
  public readonly payload: [string, string][];

  constructor(requestId: string, payload: [string, string][]) {
    this.requestId = requestId;
    this.payload = payload;
  }
}
