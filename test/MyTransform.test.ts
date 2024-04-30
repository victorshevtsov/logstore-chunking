import { MessageRef } from "@streamr/protocol";
import { PassThrough, pipeline } from "stream";
import { MyTransform } from "../src/MyTransform";
import { QueryParams } from "../src/QueryParams";
import { Storage } from "../src/Storage";
import { fillStorageWithRange } from "./test-utils";

describe("MyTransform", () => {
  test("query", (done) => {

    const storage = new Storage();
    fillStorageWithRange(storage, 100200300, 100200399);
    const queryParams: QueryParams = {
      streamId: "test-stream",
      from: new MessageRef(100200300, 0),
      to: new MessageRef(100200399, 0),
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
