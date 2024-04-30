import { MessageRef } from "@streamr/protocol";
import { PassThrough, pipeline } from "stream";
import { QueryParams } from "../src/QueryParams";
import { Storage } from "../src/Storage";
import { STREAM_ID, fillStorageWithRange } from "./test-utils";

describe("Query", () => {
  test("simple", (done) => {
    const storage = new Storage();
    fillStorageWithRange(storage, 100200300, 100200309);

    const queryParams: QueryParams = {
      streamId: STREAM_ID,
      from: new MessageRef(100200301, 0),
      to: new MessageRef(100200305, 0),
    };
    const queryStream = storage.query(queryParams);

    const passThrough = new PassThrough({ objectMode: true });

    const queryResult: any[] = [];
    queryStream.on("data", (data) => {
      queryResult.push(data);
    });

    pipeline(queryStream, passThrough, (err) => {
      done(err);
      expect(queryResult).toHaveLength(5);
    });
  });
});
