import { toEthereumAddress } from "@streamr/utils";
import data from "../data/data_5.json";
import { QueryAggregator } from "../src/QueryAggregator";
import { QueryParams } from "../src/QueryParams";
import { Storage } from "../src/Storage";
import { createQueryResponse } from "./test-utils";

describe("QueryAggregator", () => {
  test("Works with a single Foreign Node", async () => {
    const storage = new Storage(data);
    const foreignNode1 = toEthereumAddress("0x2222222222222222222222222222222222222222");
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

    const messages: any[] = [];
    const queryAggregator = new QueryAggregator(storage, queryParams, [foreignNode1]);

    // setImmediate(() => {
    queryAggregator.onResponse(
      foreignNode1,
      createQueryResponse(requestId, [data[0], data[1]], false)
    );

    queryAggregator.onResponse(
      foreignNode1,
      createQueryResponse(requestId, [data[2], data[3]], false)
    );

    queryAggregator.onResponse(
      foreignNode1,
      createQueryResponse(requestId, [data[4]], true)
    );
    // });

    for await (const message of queryAggregator) {
      messages.push(message);
    }

    expect(messages).toHaveLength(5);
  });
});
