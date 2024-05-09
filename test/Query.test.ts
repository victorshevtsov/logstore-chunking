import { PassThrough, pipeline } from "stream";
import { DatabaseAdapter } from "../src/DatabaseAdapter";
import { STREAM_ID, STREAM_PARTITION, fillStorageWithRange } from "./test-utils";

describe("Query", () => {
  test("Range", (done) => {
    const database = new DatabaseAdapter();
    fillStorageWithRange(database, 100200300, 100200309);

    const queryStream = database.queryRange(
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
