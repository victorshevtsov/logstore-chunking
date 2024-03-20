import { toEthereumAddress } from "@streamr/utils";
import data from "../data/data_5.json";
import { QueryAggregator } from "../src/QueryAggregator";
import { ChunkCallback } from "../src/QueryChipper";
import { QueryParams } from "../src/QueryParams";
import { Storage } from "../src/Storage";
import { createQueryResponse } from "./test-utils";

class TestSteps {
  private readonly steps: Function[] = [];

  public add(step: Function) {
    this.steps.push(step);
    return this;
  }

  public run(done: jest.DoneCallback, stepIndex: number = 0) {
    if (stepIndex >= this.steps.length) {
      done();
      return;
    }

    setImmediate(() => {
      this.steps[stepIndex]();
      this.run(done, stepIndex + 1);
    });
  }
}

describe("QueryAggregator aggregates messages from", () => {

  const requestId = "request-001";
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

  let chunks: any[];
  let messages: any[];
  let isFinished: boolean;

  const chunkCallback: ChunkCallback = (chunk) => {
    chunks.push(chunk);
  }

  beforeEach(() => {
    chunks = [];
    messages = [];
    isFinished = false;
  });

  describe("a standalone node", () => {

    test("if there is no data for the query", (done) => {
      const storage = new Storage([]);
      const queryAggregator = new QueryAggregator(storage, queryParams, [], chunkCallback);
      queryAggregator.on("data", (data) => messages.push(data));
      queryAggregator.on("finish", () => isFinished = true);

      new TestSteps()
        .add(() => expect(chunks).toHaveLength(1))
        .add(() => expect(messages).toHaveLength(0))
        .add(() => expect(isFinished).toBeTruthy())
        .run(done);
    });

    test("if there is data for the query", (done) => {
      const storage = new Storage(data);
      const queryAggregator = new QueryAggregator(storage, queryParams, [], chunkCallback);
      queryAggregator.on("data", (data) => messages.push(data));
      queryAggregator.on("finish", () => isFinished = true);

      new TestSteps()
        .add(() => expect(chunks).toHaveLength(3))
        .add(() => expect(messages).toHaveLength(5))
        .add(() => expect(isFinished).toBeTruthy())
        .run(done);
    });
  })

  describe("a network of a primary node and 1 foreign node", () => {
    const foreignNode1 = toEthereumAddress("0xffffffffffffffffffffffffffffffff00000001");

    describe("when primary node has queried data", () => {
      const storage = new Storage(data);
      let queryAggregator: QueryAggregator;

      beforeEach(() => {
        messages = [];
        queryAggregator = new QueryAggregator(storage, queryParams, [foreignNode1], chunkCallback);
        queryAggregator.on("data", (data) => messages.push(data));
        queryAggregator.on("finish", () => isFinished = true);
      });

      test("foreign node responds with an empty chunk", (done) => {
        new TestSteps()
          .add(() => expect(messages).toHaveLength(0))
          .add(() => queryAggregator.onResponse(foreignNode1, createQueryResponse(requestId, [], true)))
          .add(() => expect(messages).toHaveLength(5))
          .add(() => expect(isFinished).toBeTruthy())
          .run(done);
      });

      test("foreign node respondes with 1 chunk of whole data folllowed by an empty chunk ", (done) => {
        new TestSteps()
          .add(() => expect(messages).toHaveLength(0))
          .add(() => queryAggregator.onResponse(foreignNode1, createQueryResponse(requestId, [...data], false)))
          .add(() => expect(messages).toHaveLength(5))
          .add(() => queryAggregator.onResponse(foreignNode1, createQueryResponse(requestId, [], true)))
          .add(() => expect(messages).toHaveLength(5))
          .add(() => expect(isFinished).toBeTruthy())
          .run(done);
      });

      test("foreign node respondes with 2 chunks", (done) => {
        new TestSteps()
          .add(() => expect(messages).toHaveLength(0))
          .add(() => queryAggregator.onResponse(foreignNode1, createQueryResponse(requestId, [data[0], data[1], data[2]], false)))
          .add(() => expect(messages).toHaveLength(3))
          .add(() => queryAggregator.onResponse(foreignNode1, createQueryResponse(requestId, [data[3], data[4]], true)))
          .add(() => expect(messages).toHaveLength(5))
          .add(() => expect(isFinished).toBeTruthy())
          .run(done);
      });

      test("foreign node respondes with 3 chunks", (done) => {
        new TestSteps()
          .add(() => expect(messages).toHaveLength(0))
          .add(() => queryAggregator.onResponse(foreignNode1, createQueryResponse(requestId, [...data], true)))
          .add(() => expect(messages).toHaveLength(5))
          .add(() => expect(isFinished).toBeTruthy())
          .run(done);
      });
    });

    describe("primary node doesn't have queried data", () => {
      const storage = new Storage([]);
      let queryAggregator: QueryAggregator;
      //TODO:  Propagation test
    });
  });

  describe("a network of a primary node and 2 foreign nodes", () => {
    const foreignNode1 = toEthereumAddress("0xffffffffffffffffffffffffffffffff00000001");
    const foreignNode2 = toEthereumAddress("0xffffffffffffffffffffffffffffffff00000002");

    describe("when primary node has queried data", () => {
      const storage = new Storage(data);
      let queryAggregator: QueryAggregator;

      beforeEach(() => {
        messages = [];
        queryAggregator = new QueryAggregator(storage, queryParams, [foreignNode1, foreignNode2], chunkCallback);
        queryAggregator.on("data", (data) => messages.push(data));
        queryAggregator.on("finish", () => isFinished = true);
      });

      test("both foreign nodes respond with an empty chunk", (done) => {
        new TestSteps()
          .add(() => expect(messages).toHaveLength(0))
          .add(() => queryAggregator.onResponse(foreignNode1, createQueryResponse(requestId, [], true)))
          .add(() => expect(messages).toHaveLength(0))
          .add(() => queryAggregator.onResponse(foreignNode2, createQueryResponse(requestId, [], true)))
          .add(() => expect(messages).toHaveLength(5))
          .add(() => expect(isFinished).toBeTruthy())
          .run(done);
      });

      test("both foreign nodes respond with a full chunk", (done) => {
        new TestSteps()
          .add(() => expect(messages).toHaveLength(0))
          .add(() => queryAggregator.onResponse(foreignNode1, createQueryResponse(requestId, [...data], true)))
          .add(() => expect(messages).toHaveLength(0))
          .add(() => queryAggregator.onResponse(foreignNode2, createQueryResponse(requestId, [...data], true)))
          .add(() => expect(messages).toHaveLength(5))
          .add(() => expect(isFinished).toBeTruthy())
          .run(done);
      });

      test("hte 1st foreign node responds with a full chunk but the 2nd with an empty one", (done) => {
        new TestSteps()
          .add(() => expect(messages).toHaveLength(0))
          .add(() => queryAggregator.onResponse(foreignNode1, createQueryResponse(requestId, [...data], true)))
          .add(() => expect(messages).toHaveLength(0))
          .add(() => queryAggregator.onResponse(foreignNode2, createQueryResponse(requestId, [], true)))
          .add(() => expect(messages).toHaveLength(5))
          .add(() => expect(isFinished).toBeTruthy())
          .run(done);
      });

      test("hte 1st foreign node responds with ae empty chunk but the 2nd with a full one", (done) => {
        new TestSteps()
          .add(() => expect(messages).toHaveLength(0))
          .add(() => queryAggregator.onResponse(foreignNode1, createQueryResponse(requestId, [], true)))
          .add(() => expect(messages).toHaveLength(0))
          .add(() => queryAggregator.onResponse(foreignNode2, createQueryResponse(requestId, [...data], true)))
          .add(() => expect(messages).toHaveLength(5))
          .add(() => expect(isFinished).toBeTruthy())
          .run(done);
      });

      test("the foreign nodes respond with several chnks of the same queried data", (done) => {
        new TestSteps()
          .add(() => expect(messages).toHaveLength(0))
          .add(() => queryAggregator.onResponse(foreignNode1, createQueryResponse(requestId, [data[0]], false)))
          .add(() => expect(messages).toHaveLength(0))
          .add(() => queryAggregator.onResponse(foreignNode2, createQueryResponse(requestId, [data[0], data[1]], false)))
          .add(() => expect(messages).toHaveLength(1))
          .add(() => queryAggregator.onResponse(foreignNode1, createQueryResponse(requestId, [data[1], data[2]], false)))
          .add(() => expect(messages).toHaveLength(2))
          .add(() => queryAggregator.onResponse(foreignNode2, createQueryResponse(requestId, [data[2]], false)))
          .add(() => expect(messages).toHaveLength(3))
          .add(() => queryAggregator.onResponse(foreignNode1, createQueryResponse(requestId, [data[3], data[4]], true)))
          .add(() => expect(messages).toHaveLength(3))
          .add(() => queryAggregator.onResponse(foreignNode2, createQueryResponse(requestId, [data[3]], false)))
          .add(() => expect(messages).toHaveLength(4))
          .add(() => queryAggregator.onResponse(foreignNode2, createQueryResponse(requestId, [data[4]], true)))
          .add(() => expect(messages).toHaveLength(5))
          .add(() => expect(isFinished).toBeTruthy())
          .run(done);
      });

    });

    describe("primary node doesn't have queried data", () => {
      const storage = new Storage([]);
      let queryAggregator: QueryAggregator;
      //TODO:  Propagation test
    });
  });
});
