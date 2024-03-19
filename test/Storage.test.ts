import data from "../data/data_5.json";
import { Storage } from "../src/Storage";

const streamId = "0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a/pulse";

describe("Storage", () => {
  const storage = new Storage(data);

  test("query", async () => {
    const queryStream = storage.query({
      streamId,
      from: {
        timestamp: 1710336591127,
      },
      to: {
        timestamp: 1710357184411,
      },
    });

    const messages = [];
    for await (const message of queryStream) {
      messages.push(message);
    }

    expect(messages).toHaveLength(5);
  });

  test("query", async () => {
    const queryStream = storage.query({
      streamId,
      from: {
        timestamp: 1710336591127,
        sequenceNumber: 1,
      },
      to: {
        timestamp: 1710357181407,
        sequenceNumber: 1,
      },
    });

    const messages = [];
    for await (const message of queryStream) {
      messages.push(message);
    }

    expect(messages).toHaveLength(1);
  });
});
