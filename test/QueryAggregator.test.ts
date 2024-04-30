import { MessageRef } from "@streamr/protocol";
import { convertStreamMessageToBytes } from "@streamr/trackerless-network";
import { toEthereumAddress } from "@streamr/utils";
import { PassThrough, pipeline } from "stream";
import { QueryAggregator } from "../src/QueryAggregator";
import { QueryParams } from "../src/QueryParams";
import { Storage } from "../src/Storage";
import { STREAM_ID, createQueryPropagation, createQueryResponse, mockStreamMessageRange } from "./test-utils";

describe("QueryAggregator aggregates messages from", () => {

  const requestId = "request-001";
  const queryParams: QueryParams = {
    streamId: STREAM_ID,
    from: new MessageRef(100200300, 0),
    to: new MessageRef(100200301, 0),
  };


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
      queryAggregator = new QueryAggregator(storage, queryParams, [], responseChunkCallbackMock);
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
      queryAggregator = new QueryAggregator(storage, queryParams, [foreignNode1], responseChunkCallbackMock);
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

      queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(requestId, [], true));
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

        queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(requestId, [data[0], data[1], data[2]], true));
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

        queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(requestId, [data[0], data[1]], false));
        queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(requestId, [data[2]], true));
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

        queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(requestId, [data[0]], false));
        queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(requestId, [data[1]], false));
        queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(requestId, [data[2]], true));
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

      queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(requestId, [], true));
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

      queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(requestId, [data[0]], true));
      queryAggregator.onPropagation(foreignNode1, createQueryPropagation(requestId, [data[0]], true));
    });
  });

  describe("a network of a primary node and 2 foreign nodes", () => {
    const foreignNode1 = toEthereumAddress("0xffffffffffffffffffffffffffffffff00000001");
    const foreignNode2 = toEthereumAddress("0xffffffffffffffffffffffffffffffff00000002");

    beforeEach(() => {
      queryAggregator = new QueryAggregator(storage, queryParams, [foreignNode1, foreignNode2], responseChunkCallbackMock);
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

      queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(requestId, [], true));
      queryAggregator.onForeignResponse(foreignNode2, createQueryResponse(requestId, [], true));
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

      queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(requestId, [data[0]], true));
      queryAggregator.onForeignResponse(foreignNode2, createQueryResponse(requestId, [data[0]], true));
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

      queryAggregator.onForeignResponse(foreignNode1, createQueryResponse(requestId, [data[1]], true));
      queryAggregator.onForeignResponse(foreignNode2, createQueryResponse(requestId, [data[2]], true));
    });
  });
});
