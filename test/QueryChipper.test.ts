import { PassThrough, pipeline } from "stream";
import { QueryChipper } from "../src/QueryChipper";

describe("QueryChipper", () => {
  let chunkCallback: jest.Mock;
  let sourceStreamMock: PassThrough;
  let queryChipper: QueryChipper;

  beforeEach(() => {
    chunkCallback = jest.fn().mockImplementation();
    sourceStreamMock = new PassThrough({ objectMode: true });
  });

  describe("chunk length limit: 2", () => {

    beforeEach(() => {
      queryChipper = new QueryChipper(chunkCallback, { lengthLimit: 2 });
    });

    test("calls the callback with an empty final chunk", (done) => {
      pipeline(sourceStreamMock, queryChipper, (err) => {
        if (err) {
          done(err);
          return;
        }

        try {
          expect(chunkCallback).toHaveBeenNthCalledWith(1, [], true);
          done();
        } catch (error) {
          done(error);
        }
      });

      sourceStreamMock.push(null);
    });

    test("calls the callback with a final chunk of 1 element", (done) => {
      pipeline(sourceStreamMock, queryChipper, (err) => {
        if (err) {
          done(err);
          return;
        }

        try {
          expect(chunkCallback).toHaveBeenNthCalledWith(1, ["element-00"], true);
          done();
        } catch (error) {
          done(error);
        }
      });
      sourceStreamMock.push("element-00");
      sourceStreamMock.push(null);
    });

    test("calls the callback with a chunk of 2 elements followed by an empty final chunk", (done) => {
      pipeline(sourceStreamMock, queryChipper, (err) => {
        if (err) {
          done(err);
          return;
        }

        try {
          expect(chunkCallback).toHaveBeenNthCalledWith(1, ["element-00", "element-01"], false);
          expect(chunkCallback).toHaveBeenNthCalledWith(2, [], true);
          done();
        } catch (error) {
          done(error);
        }
      });

      sourceStreamMock.push("element-00");
      sourceStreamMock.push("element-01");
      sourceStreamMock.push(null);
    });

    test("calls the callback with a chunk of 2 elements followed by a final chunk of 1 element", (done) => {
      pipeline(sourceStreamMock, queryChipper, (err) => {
        if (err) {
          done(err);
          return;
        }

        try {
          expect(chunkCallback).toHaveBeenNthCalledWith(1, ["element-00", "element-01"], false);
          expect(chunkCallback).toHaveBeenNthCalledWith(2, ["element-02"], true);
          done();
        } catch (error) {
          done(error);
        }
      });

      sourceStreamMock.push("element-00");
      sourceStreamMock.push("element-01");
      sourceStreamMock.push("element-02");
      sourceStreamMock.push(null);
    });
  });

  describe("chunk size limit: 20", () => {

    beforeEach(() => {
      queryChipper = new QueryChipper(chunkCallback, { sizeLimit: 20 });
    });

    test("calls the callback with a chunk of 2 elements followed by an empty final chunk", (done) => {
      pipeline(sourceStreamMock, queryChipper, (err) => {
        if (err) {
          done(err);
          return;
        }

        try {
          expect(chunkCallback).toHaveBeenNthCalledWith(1, ["element-00", "element-01"], false);
          expect(chunkCallback).toHaveBeenNthCalledWith(2, [], true);
          done();
        } catch (error) {
          done(error);
        }
      });

      sourceStreamMock.push("element-00");
      sourceStreamMock.push("element-01");
      sourceStreamMock.push(null);
    });

  });

  // TODO: Cover the cases with error handling

});
