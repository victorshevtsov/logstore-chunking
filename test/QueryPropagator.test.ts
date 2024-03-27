import { PassThrough, Readable } from "stream";
import data from "../data/data_5.json";
import { QueryParams } from "../src/QueryParams";
import { QueryPropagator } from "../src/QueryPropagator";
import { Storage } from "../src/Storage";
import { createQueryResponse } from "./test-utils";

describe("QueryPropagator", () => {
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
  let propagationChunkCallbackMock: jest.Mock;
  let queryStreamMock: Readable;
  let queryByMessageIdsStreamMock1: Readable;
  let queryByMessageIdsStreamMock2: Readable;
  let queryPropagator: QueryPropagator;

  beforeEach(() => {
    responseChunkCallbackMock = jest.fn().mockImplementation();
    propagationChunkCallbackMock = jest.fn().mockImplementation();
    queryStreamMock = new Readable({ objectMode: true });
    queryByMessageIdsStreamMock1 = new PassThrough({ objectMode: true });
    queryByMessageIdsStreamMock2 = new PassThrough({ objectMode: true });

    const storage = jest.mocked<Storage>({
      query: jest.fn().mockImplementation(() => queryStreamMock),
      queryByMessageIds: jest.fn()
        .mockImplementationOnce(() => queryByMessageIdsStreamMock1)
        .mockImplementationOnce(() => queryByMessageIdsStreamMock2),
    } as unknown as Storage, { shallow: true });

    queryPropagator = new QueryPropagator(storage, queryParams, responseChunkCallbackMock, propagationChunkCallbackMock);
  });

  describe("storage has no data", () => {
    test("simple #1", (done) => {
      queryPropagator.onPrimaryResponse(createQueryResponse(requestId, [], true));

      queryStreamMock.push(null);

      setImmediate(() => {
        expect(responseChunkCallbackMock).toHaveBeenCalledTimes(1);
        expect(responseChunkCallbackMock).toHaveBeenNthCalledWith(1, [], true);

        expect(propagationChunkCallbackMock).toHaveBeenCalledTimes(1);
        expect(propagationChunkCallbackMock).toHaveBeenCalledWith([], true);

        done();
      });
    });

    test("simple #2", (done) => {
      queryPropagator.onPrimaryResponse(createQueryResponse(requestId, [data[0]], true));

      queryStreamMock.push(data[0]);
      queryStreamMock.push(null);

      setImmediate(() => {
        expect(responseChunkCallbackMock).toHaveBeenCalledTimes(1);
        expect(responseChunkCallbackMock).toHaveBeenNthCalledWith(1, [data[0]], true);

        expect(propagationChunkCallbackMock).toHaveBeenCalledTimes(1);
        expect(propagationChunkCallbackMock).toHaveBeenCalledWith([], true);

        done();
      });
    });

    test("simple #5", (done) => {
      queryPropagator.onPrimaryResponse(createQueryResponse(requestId, [data[0], data[1]], true));

      queryStreamMock.push(data[0]);
      queryStreamMock.push(data[1]);
      queryStreamMock.push(null);

      setImmediate(() => {
        expect(responseChunkCallbackMock).toHaveBeenCalledTimes(1);
        expect(responseChunkCallbackMock).toHaveBeenNthCalledWith(1, [data[0], data[1]], true);

        expect(propagationChunkCallbackMock).toHaveBeenCalledTimes(1);
        expect(propagationChunkCallbackMock).toHaveBeenCalledWith([], true);;

        done();
      });
    });

    test("simple #6", (done) => {
      queryPropagator.onPrimaryResponse(createQueryResponse(requestId, [], true));

      queryStreamMock.push(data[0]);
      queryStreamMock.push(null);

      queryByMessageIdsStreamMock1.push(data[0]);
      queryByMessageIdsStreamMock1.push(null);

      setImmediate(() => {
        expect(responseChunkCallbackMock).toHaveBeenCalledTimes(1);
        expect(responseChunkCallbackMock).toHaveBeenNthCalledWith(1, [data[0]], true);

        expect(propagationChunkCallbackMock).toHaveBeenCalledTimes(1);
        expect(propagationChunkCallbackMock).toHaveBeenNthCalledWith(1, [data[0]], true);

        done();
      });
    });

    test("simple #7", (done) => {
      queryPropagator.onPrimaryResponse(createQueryResponse(requestId, [], true));

      queryStreamMock.push(data[0]);
      queryStreamMock.push(data[1]);
      queryStreamMock.push(null);

      queryByMessageIdsStreamMock1.push(data[0]);
      queryByMessageIdsStreamMock1.push(null);

      queryByMessageIdsStreamMock2.push(data[1]);
      queryByMessageIdsStreamMock2.push(null);

      setImmediate(() => {
        expect(responseChunkCallbackMock).toHaveBeenCalledTimes(1);
        expect(responseChunkCallbackMock).toHaveBeenNthCalledWith(1, [data[0], data[1]], true);

        expect(propagationChunkCallbackMock).toHaveBeenCalledTimes(1);
        expect(propagationChunkCallbackMock).toHaveBeenNthCalledWith(1, [data[0], data[1]], true);

        done();
      });
    });

    test("simple #4", (done) => {
      queryPropagator.onPrimaryResponse(createQueryResponse(requestId, [data[0]], true));

      queryStreamMock.push(data[0]);
      queryStreamMock.push(data[1]);
      queryStreamMock.push(null);

      queryByMessageIdsStreamMock1.push(data[1]);
      queryByMessageIdsStreamMock1.push(null);

      setImmediate(() => {
        expect(responseChunkCallbackMock).toHaveBeenCalledTimes(1);
        expect(responseChunkCallbackMock).toHaveBeenNthCalledWith(1, [data[0], data[1]], true);

        expect(propagationChunkCallbackMock).toHaveBeenCalledTimes(1);
        expect(propagationChunkCallbackMock).toHaveBeenNthCalledWith(1, [data[1]], true);;

        done();
      });
    });
  });
});
