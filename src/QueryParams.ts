import { MessageRef } from "@streamr/protocol";

export interface QueryParams {
  streamId: string;
  from: MessageRef;
  to: MessageRef;
}
