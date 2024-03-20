export class QueryResponse {
  public readonly requestId: string;
  public readonly isFinal: boolean;
  public readonly messageIds: string[] = [];
  // public readonly hashMap: Map<string, string>;

  constructor(requestId: string, isFinal: boolean) {
    this.requestId = requestId;
    this.isFinal = isFinal;
    // this.hashMap = new Map();
  }
}
