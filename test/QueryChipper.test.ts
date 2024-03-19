import { MessageID } from "@streamr/protocol";
import { pipeline } from "stream";
import data from "../data/data_5.json";
import { ChunkCallback, QueryChipper } from "../src/QueryChipper";
import { QueryParams } from "../src/QueryParams";
import { Storage } from "../src/Storage";

describe("QueryChipper", () => {
  test("Works with a single Foreign Node", (done) => {
    const storage = new Storage(data);
    const queryParams: QueryParams = {
      streamId: "0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a/pulse",
      from: {
        timestamp: 1710336591127,
        sequenceNumber: 0,
      },
      to: {
        timestamp: 1710357184411,
        sequenceNumber: 0,
      }
    };

    const chunks: MessageID[][] = [];
    const chunkCallback: ChunkCallback = (chunk: MessageID[]) => {
      chunks.push(chunk);
      return null;
    }

    const queryStream = storage.query(queryParams);
    const queryChipper = new QueryChipper(chunkCallback);

    pipeline(queryStream, queryChipper, (err) => {
      if (err) {
        console.error(err);
      }

      expect(chunks).toHaveLength(3);

      done();
    });

  });
});
