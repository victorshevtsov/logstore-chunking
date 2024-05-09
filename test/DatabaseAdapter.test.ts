import { DatabaseAdapter } from "../src/DatabaseAdapter";
import { STREAM_ID, STREAM_PARTITION, fillStorageWithRange, mockStreamMessage } from "./test-utils";

describe("DatabaseAdapter", () => {
  const databaseAdapter = new DatabaseAdapter();

  beforeEach(() => {
    fillStorageWithRange(databaseAdapter, 100200300, 100200400);
  });

  test("query", async () => {
    const queryStream = databaseAdapter.queryRange(
      STREAM_ID,
      STREAM_PARTITION,
      100200300,
      0,
      100200309,
      0,
    );

    const messages: any[] = [];
    for await (const message of queryStream) {
      messages.push(message);
    }

    expect(messages).toHaveLength(10);
  });

  test("query", async () => {

    const msg = mockStreamMessage({ timestamp: 100200300, sequenceNumber: 1 });
    databaseAdapter.store(msg);

    const queryStream = databaseAdapter.queryRange(
      STREAM_ID,
      STREAM_PARTITION,
      100200300,
      1,
      100200300,
      1,
    );

    const messages: any[] = [];
    for await (const message of queryStream) {
      messages.push(message);
    }

    expect(messages).toHaveLength(1);
  });
});
