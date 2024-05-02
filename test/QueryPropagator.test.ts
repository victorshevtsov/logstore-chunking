import { MessageRef } from "@streamr/protocol";
import { convertStreamMessageToBytes } from "@streamr/trackerless-network";
import { PassThrough, Readable } from "stream";
import { QueryPropagator } from "../src/QueryPropagator";
import { Storage } from "../src/Storage";
import { QueryRequest, QueryType } from "../src/protocol/QueryRequest";
import { REQUEST_ID, STREAM_ID, STREAM_PARTITION, createQueryResponse, mockStreamMessageRange } from "./test-utils";

describe("QueryPropagator", () => {
  const data = Array
    .from(mockStreamMessageRange(100200300, 100200303))
    .map(m => convertStreamMessageToBytes(m));

  const queryRequest = new QueryRequest({
    requestId: REQUEST_ID,
    consumerId: "consumerId",
    streamId: STREAM_ID,
    partition: STREAM_PARTITION,
    queryType: QueryType.Range,
    queryOptions: {
      queryType: QueryType.Range,
      from: new MessageRef(100200300, 0),
      to: new MessageRef(100200300, 0),
    }
  });

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

    queryPropagator = new QueryPropagator(storage, queryRequest, responseChunkCallbackMock, propagationChunkCallbackMock);
  });

  describe("storage has no data", () => {
    test("simple #1", (done) => {
      queryPropagator.onPrimaryResponse(createQueryResponse(REQUEST_ID, [], true));

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
      queryPropagator.onPrimaryResponse(createQueryResponse(REQUEST_ID, [data[0]], true));

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
      queryPropagator.onPrimaryResponse(createQueryResponse(REQUEST_ID, [data[0], data[1]], true));

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
      queryPropagator.onPrimaryResponse(createQueryResponse(REQUEST_ID, [], true));

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
      queryPropagator.onPrimaryResponse(createQueryResponse(REQUEST_ID, [], true));

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
      queryPropagator.onPrimaryResponse(createQueryResponse(REQUEST_ID, [data[0]], true));

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
