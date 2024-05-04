import { Storage } from "../src/Storage";
import { STREAM_ID, STREAM_PARTITION, fillStorageWithRange, mockStreamMessage } from "./test-utils";

describe("Storage", () => {
  const storage = new Storage();

  beforeEach(() => {
    fillStorageWithRange(storage, 100200300, 100200400);
  });

  test("query", async () => {
    const queryStream = storage.queryRange(
      STREAM_ID,
      STREAM_PARTITION,
      100200300,
      0,
      100200309,
      0,
    );

    const messages = [];
    for await (const message of queryStream) {
      messages.push(message);
    }

    expect(messages).toHaveLength(10);
  });

  test("query", async () => {

    const msg = mockStreamMessage({ timestamp: 100200300, sequenceNumber: 1 });
    storage.store(msg);

    const queryStream = storage.queryRange(
      STREAM_ID,
      STREAM_PARTITION,
      100200300,
      1,
      100200300,
      1,
    );

    const messages = [];
    for await (const message of queryStream) {
      messages.push(message);
    }

    expect(messages).toHaveLength(1);
  });
});
