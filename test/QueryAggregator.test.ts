import { toEthereumAddress } from "@streamr/utils";
import { PassThrough, pipeline } from "stream";
import data from "../data/data_5.json";
import { QueryAggregator } from "../src/QueryAggregator";
import { QueryParams } from "../src/QueryParams";
import { Storage } from "../src/Storage";
import { createQueryResponse } from "./test-utils";

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

  let responseChunkCallbackMock: jest.Mock;

  let queryStreamMock: PassThrough;
  let responseStreamMock: PassThrough;
  let storage: Storage;
  let queryAggregator: QueryAggregator;

  beforeEach(() => {
    responseChunkCallbackMock = jest.fn().mockImplementation();
    queryStreamMock = new PassThrough({ objectMode: true });
    responseStreamMock = new PassThrough({ objectMode: true });
    storage = jest.mocked<Storage>({
      query: jest.fn().mockImplementation(() => queryStreamMock),
    } as unknown as Storage, { shallow: true });
  });

  describe("single primary node", () => {

    beforeEach(() => {
      queryAggregator = new QueryAggregator(storage, queryParams, [], responseChunkCallbackMock);
    });

    test("Test #1", (done) => {
      pipeline(queryAggregator, responseStreamMock, (err) => {
        try {
          expect(responseChunkCallbackMock).toHaveBeenCalledTimes(1);
          expect(responseChunkCallbackMock).toHaveBeenNthCalledWith(1, [], true);

          done(err);
        } catch (e) {
          done(e);
        }
      });

      queryStreamMock.push(null);
    });

    test("Test #2", (done) => {
      pipeline(queryAggregator, responseStreamMock, (err) => {
        try {
          expect(responseChunkCallbackMock).toHaveBeenCalledTimes(1);
          expect(responseChunkCallbackMock).toHaveBeenNthCalledWith(1, [data[0]], true);

          done(err);
        } catch (e) {
          done(e);
        }
      });

      queryStreamMock.push(data[0]);
      queryStreamMock.push(null);
    });
  });

  describe("a network of a primary node and 1 foreign node", () => {
    const foreignNode1 = toEthereumAddress("0xffffffffffffffffffffffffffffffff00000001");

    beforeEach(() => {
      queryAggregator = new QueryAggregator(storage, queryParams, [foreignNode1], responseChunkCallbackMock);
    });

    test("Test #1", (done) => {
      pipeline(queryAggregator, responseStreamMock, (err) => {
        try {
          expect(responseChunkCallbackMock).toHaveBeenCalledTimes(1);
          expect(responseChunkCallbackMock).toHaveBeenNthCalledWith(1, [], true);

          done(err);
        } catch (e) {
          done(e);
        }
      });

      queryStreamMock.push(null);

      queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(requestId, [], true));
    });

    test("Test #2", (done) => {
      pipeline(queryAggregator, responseStreamMock, (err) => {
        try {
          expect(responseChunkCallbackMock).toHaveBeenCalledTimes(1);
          expect(responseChunkCallbackMock).toHaveBeenNthCalledWith(1, [data[1]], true);

          done(err);
        } catch (e) {
          done(e);
        }
      });

      queryStreamMock.push(data[1]);
      queryStreamMock.push(null);

      queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(requestId, [], true));
    });

    test("Test #3", (done) => {
      pipeline(queryAggregator, responseStreamMock, (err) => {
        try {
          expect(responseChunkCallbackMock).toHaveBeenCalledTimes(1);
          expect(responseChunkCallbackMock).toHaveBeenNthCalledWith(1, [data[1]], true);

          done(err);
        } catch (e) {
          done(e);
        }
      });

      queryStreamMock.push(data[1]);
      queryStreamMock.push(null);

      queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(requestId, [data[1]], true));
    });   
  });
});
