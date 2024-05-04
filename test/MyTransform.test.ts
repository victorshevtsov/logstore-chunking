import { PassThrough, pipeline } from "stream";
import { MyTransform } from "../src/MyTransform";
import { Storage } from "../src/Storage";
import { STREAM_ID, STREAM_PARTITION, fillStorageWithRange } from "./test-utils";

describe("MyTransform", () => {
  test("query", (done) => {

    const storage = new Storage();
    fillStorageWithRange(storage, 100200300, 100200302);

    const queryStream = storage.queryRange(
      STREAM_ID,
      STREAM_PARTITION,
      100200300,
      0,
      100200309,
      0
    );
    const myTransformStream = new MyTransform();
    const responseStream = new PassThrough();

    pipeline(queryStream, myTransformStream, responseStream, (err) => {
      if (err) {
        console.error(err);
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

    // responseStream.on("data", (data) => {
    //   console.log("data", data);
    // })

  });
});
