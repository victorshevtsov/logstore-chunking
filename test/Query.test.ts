import { PassThrough, pipeline } from "stream";
import { Storage } from "../src/Storage";
import { STREAM_ID, STREAM_PARTITION, fillStorageWithRange } from "./test-utils";

describe("Query", () => {
  test("simple", (done) => {
    const storage = new Storage();
    fillStorageWithRange(storage, 100200300, 100200309);

    const queryStream = storage.query(
      STREAM_ID,
      STREAM_PARTITION,
      100200301,
      0,
      100200305,
      0
    );

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
