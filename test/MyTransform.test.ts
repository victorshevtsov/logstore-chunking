import { Writable, pipeline, PassThrough } from "stream";
import data from "../data/data_5.json";
import { MyTransform } from "../src/MyTransform";
import { QueryParams } from "../src/QueryParams";
import { Storage } from "../src/Storage";

describe("MyTransform", () => {
  test("query", (done) => {

    const storage = new Storage(data);
    const queryParams: QueryParams = {
      streamId: "test-stream",
      from: { timestamp: 100200300 },
      to: { timestamp: 100200399 },
    };

    const queryStream = storage.query(queryParams);
    const myTransformStream = new MyTransform();
    const responseStream = new PassThrough();

    pipeline(queryStream, myTransformStream, responseStream, (err) => {
      if (err) {
        console.error(err); 6
      }

      done();
    });

    // responseStream.on("pipe", () => {
    //   console.log("pipe");
    // })

    // responseStream.on("unpipe", () => {
    //   console.log("unpipe");
    // })

    // responseStream.on("finish", () => {
    //   console.log("finish");
    // })

    // responseStream.on("close", () => {
    //   console.log("close");
    // })

    // responseStream.on("data", () => {
    //   console.log("data");
    // })

  });
});
