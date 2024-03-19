import { QueryParams } from "./QueryParams";

export class QueryRequest {
  public readonly requestId: string;
  public readonly params: QueryParams

  constructor(requestId: string, params: QueryParams) {
    this.requestId = requestId;
    this.params = params;
  }
}
