import { PassThrough, pipeline } from "stream";
import data from "../data/data_5.json";
import { QueryParams } from "../src/QueryParams";
import { Storage } from "../src/Storage";

describe("Query", () => {
  test("simple", (done) => {
    const queryParams: QueryParams = {
      streamId: "0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a/pulse",
      from: { timestamp: 1710336591127 },
      to: { timestamp: 1710357184411 },
    };

    const storage = new Storage(data);
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
