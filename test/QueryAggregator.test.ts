import { MessageRef } from "@streamr/protocol";
import { convertStreamMessageToBytes } from "@streamr/trackerless-network";
import { toEthereumAddress } from "@streamr/utils";
import { PassThrough, pipeline } from "stream";
import { QueryAggregator } from "../src/QueryAggregator";
import { Storage } from "../src/Storage";
import { QueryRequest, QueryType } from "../src/protocol/QueryRequest";
import { REQUEST_ID, STREAM_ID, STREAM_PARTITION, createQueryPropagation, createQueryResponse, mockStreamMessageRange } from "./test-utils";

describe("QueryAggregator aggregates messages from", () => {

  const queryRequest = new QueryRequest({
    requestId: REQUEST_ID,
    consumerId: "consumerId",
    streamId: STREAM_ID,
    partition: STREAM_PARTITION,
    queryType: QueryType.Range,
    queryOptions: {
      queryType: QueryType.Range,
      from: new MessageRef(100200300, 0),
      to: new MessageRef(100200301, 0),
    }
  });

  let data: Uint8Array[];
  let responseChunkCallbackMock: jest.Mock;

  let queryStreamMock: PassThrough;
  let responseStreamMock: PassThrough;
  let storage: Storage;
  let queryAggregator: QueryAggregator;

  beforeEach(() => {
    data = Array
      .from(mockStreamMessageRange(100200300, 100200309))
      .map(m => convertStreamMessageToBytes(m));

    responseChunkCallbackMock = jest.fn().mockImplementation();
    queryStreamMock = new PassThrough({ objectMode: true });
    responseStreamMock = new PassThrough({ objectMode: true });
    storage = jest.mocked<Storage>({
      query: jest.fn().mockImplementation(() => queryStreamMock),
      store: jest.fn().mockImplementation(),
    } as unknown as Storage, { shallow: true });
  });

  describe("single primary node", () => {

    beforeEach(() => {
      queryAggregator = new QueryAggregator(storage, queryRequest, [], responseChunkCallbackMock);
    });

    test("if there is no data for the query", (done) => {
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

    test("if there is data for the query", (done) => {
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
      queryAggregator = new QueryAggregator(storage, queryRequest, [foreignNode1], responseChunkCallbackMock);
    });

    test("if there is no data for the query", (done) => {
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

      queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(REQUEST_ID, [], true));
    });

    describe("if the primary node has the same data as the foreign node ", () => {
      test("and the foreign node responds with a single response", (done) => {
        pipeline(queryAggregator, responseStreamMock, (err) => {
          try {
            expect(responseChunkCallbackMock).toHaveBeenCalledTimes(1);
            expect(responseChunkCallbackMock).toHaveBeenNthCalledWith(1, [data[0], data[1], data[2]], true);

            done(err);
          } catch (e) {
            done(e);
          }
        });

        queryStreamMock.push(data[0]);
        queryStreamMock.push(data[1]);
        queryStreamMock.push(data[2]);
        queryStreamMock.push(null);

        queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(REQUEST_ID, [data[0], data[1], data[2]], true));
      });

      test("and the foreign node responds with 2 responses", (done) => {
        pipeline(queryAggregator, responseStreamMock, (err) => {
          try {
            expect(responseChunkCallbackMock).toHaveBeenCalledTimes(1);
            expect(responseChunkCallbackMock).toHaveBeenNthCalledWith(1, [data[0], data[1], data[2]], true);

            done(err);
          } catch (e) {
            done(e);
          }
        });

        queryStreamMock.push(data[0]);
        queryStreamMock.push(data[1]);
        queryStreamMock.push(data[2]);
        queryStreamMock.push(null);

        queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(REQUEST_ID, [data[0], data[1]], false));
        queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(REQUEST_ID, [data[2]], true));
      });

      test("and the foreign node responds with 3 responses", (done) => {
        pipeline(queryAggregator, responseStreamMock, (err) => {
          try {
            expect(responseChunkCallbackMock).toHaveBeenCalledTimes(1);
            expect(responseChunkCallbackMock).toHaveBeenNthCalledWith(1, [data[0], data[1], data[2]], true);

            done(err);
          } catch (e) {
            done(e);
          }
        });

        queryStreamMock.push(data[0]);
        queryStreamMock.push(data[1]);
        queryStreamMock.push(data[2]);
        queryStreamMock.push(null);

        queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(REQUEST_ID, [data[0]], false));
        queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(REQUEST_ID, [data[1]], false));
        queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(REQUEST_ID, [data[2]], true));
      });
    });

    test("if the primary node has data for the query but the foreign node does not ", (done) => {
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

      queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(REQUEST_ID, [], true));
    });

    test.skip("if the primary node does not have data for the query but the foreign node does ", (done) => {
      pipeline(queryAggregator, responseStreamMock, (err) => {
        try {
          expect(responseChunkCallbackMock).toHaveBeenCalledTimes(1);
          expect(responseChunkCallbackMock).toHaveBeenNthCalledWith(1, [data[0]], true);

          done(err);
        } catch (e) {
          done(e);
        }
      });

      queryStreamMock.push(null);

      queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(REQUEST_ID, [data[0]], true));
      queryAggregator.onPropagation(foreignNode1, createQueryPropagation(REQUEST_ID, [data[0]], true));
    });
  });

  describe("a network of a primary node and 2 foreign nodes", () => {
    const foreignNode1 = toEthereumAddress("0xffffffffffffffffffffffffffffffff00000001");
    const foreignNode2 = toEthereumAddress("0xffffffffffffffffffffffffffffffff00000002");

    beforeEach(() => {
      queryAggregator = new QueryAggregator(storage, queryRequest, [foreignNode1, foreignNode2], responseChunkCallbackMock);
    });

    test("if there is no data for the query", (done) => {
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

      queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(REQUEST_ID, [], true));
      queryAggregator.onForeignResponse(foreignNode2, createQueryResponse(REQUEST_ID, [], true));
    });

    test("if the primary node has the same data as both foreign nodes ", (done) => {
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

      queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(REQUEST_ID, [data[0]], true));
      queryAggregator.onForeignResponse(foreignNode2, createQueryResponse(REQUEST_ID, [data[0]], true));
    });

    test("if the primary node has all the data for the query but the foreign nodes have it partially", (done) => {
      pipeline(queryAggregator, responseStreamMock, (err) => {
        try {
          expect(responseChunkCallbackMock).toHaveBeenCalledTimes(1);
          expect(responseChunkCallbackMock).toHaveBeenNthCalledWith(1, [data[0], data[1], data[2]], true);

          done(err);
        } catch (e) {
          done(e);
        }
      });

      queryStreamMock.push(data[0]);
      queryStreamMock.push(data[1]);
      queryStreamMock.push(data[2]);
      queryStreamMock.push(null);

      queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(REQUEST_ID, [data[1]], true));
      queryAggregator.onForeignResponse(foreignNode2, createQueryResponse(REQUEST_ID, [data[2]], true));
    });
  });
});
