import { ContentType, EncryptionType, MessageID, SignatureType, StreamMessage, toStreamID } from "@streamr/protocol";
import { convertBytesToStreamMessage } from "@streamr/trackerless-network";
import { EthereumAddress, hexToBinary, toEthereumAddress, utf8ToBinary } from "@streamr/utils";
import { DatabaseAdapter } from "../src/DatabaseAdapter";
import { QueryPropagate } from "../src/protocol/QueryPropagate";
import { QueryResponse } from "../src/protocol/QueryResponse";

export const PUBLISHER_ID = "0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a";
export const STREAM_ID = `${PUBLISHER_ID}/pulse`;
export const STREAM_PARTITION = 0;
export const MSG_CHAIN_ID = "msgChainId";
export const REQUEST_ID = "request-001";
  
export function createQueryResponse(requestId: string, serializedMessages: Uint8Array[], isFinal: boolean) {
  const messageRefs = serializedMessages.map(serializedMessage =>
    convertBytesToStreamMessage(serializedMessage).messageId.toMessageRef()
  );

  return new QueryResponse(requestId, messageRefs, isFinal);
}

export function createQueryPropagate(requestId: string, serializedMessages: Uint8Array[]) {
  return new QueryPropagate(requestId, serializedMessages);
}

export function mockStreamMessage({
  streamId = STREAM_ID,
  streamPartition = STREAM_PARTITION,
  timestamp,
  sequenceNumber = 0,
  publisherId = toEthereumAddress(PUBLISHER_ID),
  msgChainId = MSG_CHAIN_ID,
}: {
  streamId?: string;
  streamPartition?: number;
  timestamp: number
  sequenceNumber?: number;
  publisherId?: EthereumAddress;
  msgChainId?: string;
}) {
  return new StreamMessage({
    messageId: new MessageID(
      toStreamID(streamId),
      streamPartition,
      timestamp,
      sequenceNumber,
      publisherId,
      msgChainId,
    ),
    content: Buffer.from(utf8ToBinary(JSON.stringify({ ts: timestamp }))),
    signature: Buffer.from(hexToBinary('0x1234')),
    contentType: ContentType.JSON,
    encryptionType: EncryptionType.NONE,
    signatureType: SignatureType.SECP256K1,
  })
}

export function* mockStreamMessageRange(from: number, to: number) {
  for (let timestamp = from; timestamp <= to; timestamp++) {
    yield mockStreamMessage({ timestamp });
  }
}

export function fillStorageWithRange(storage: DatabaseAdapter, from: number, to: number) {
  for (const streamMessage of mockStreamMessageRange(from, to)) {
    storage.store(streamMessage);
  }
}

export class TestScenario {
  private readonly steps: Function[] = [];

  public add(step: Function) {
    this.steps.push(step);
    return this;
  }

  public run(done: jest.DoneCallback, stepIndex: number = 0) {
    if (stepIndex >= this.steps.length) {
      done();
      return;
    }

    setImmediate(() => {
      this.steps[stepIndex]();
      this.run(done, stepIndex + 1);
    });
  }
}
