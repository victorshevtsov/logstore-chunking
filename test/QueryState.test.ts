import { StreamMessage } from "@streamr/protocol";
import data from "../data/data_5.json";
import { QueryState } from "../src/QeryState";

const messageIds = data
  .map(StreamMessage.deserialize)
  .map(m => m.messageId);

describe("QueryState", () => {

  describe("calculates min and ma for a set of", () => {
    let queryState: QueryState;

    beforeEach(() => {
      queryState = new QueryState();
    });

    it("1 MessageID", () => {
      queryState.addMessageId(messageIds[0]);

      expect(queryState.min?.timestamp).toEqual(messageIds[0].timestamp);
      expect(queryState.min?.sequenceNumber).toEqual(messageIds[0].sequenceNumber);
      expect(queryState.max?.timestamp).toEqual(messageIds[0].timestamp);
      expect(queryState.max?.sequenceNumber).toEqual(messageIds[0].sequenceNumber);
    });

    it("2 MessageIDs", () => {
      queryState.addMessageId(messageIds[0]);
      queryState.addMessageId(messageIds[1]);

      expect(queryState.min?.timestamp).toEqual(messageIds[0].timestamp);
      expect(queryState.min?.sequenceNumber).toEqual(messageIds[0].sequenceNumber);
      expect(queryState.max?.timestamp).toEqual(messageIds[1].timestamp);
      expect(queryState.max?.sequenceNumber).toEqual(messageIds[1].sequenceNumber);
    });

    it("3 MessageIDs", () => {
      queryState.addMessageId(messageIds[0]);
      queryState.addMessageId(messageIds[1]);
      queryState.addMessageId(messageIds[2]);

      expect(queryState.min?.timestamp).toEqual(messageIds[0].timestamp);
      expect(queryState.min?.sequenceNumber).toEqual(messageIds[0].sequenceNumber);
      expect(queryState.max?.timestamp).toEqual(messageIds[2].timestamp);
      expect(queryState.max?.sequenceNumber).toEqual(messageIds[2].sequenceNumber);
    });
  });

  describe("shrinks a set of", () => {
    let queryState: QueryState;

    beforeEach(() => {
      queryState = new QueryState();
    });

    describe("1 element", () => {
      beforeEach(() => {
        queryState.addMessageId(messageIds[0]);
      });

      it("if the slice is less than the element", () => {
        queryState.shrink({
          timestamp: messageIds[0].timestamp - 1,
          sequenceNumber: 0,
        });

        expect(queryState.min?.timestamp).toEqual(messageIds[0].timestamp);
        expect(queryState.min?.sequenceNumber).toEqual(messageIds[0].sequenceNumber);
        expect(queryState.max?.timestamp).toEqual(messageIds[0].timestamp);
        expect(queryState.max?.sequenceNumber).toEqual(messageIds[0].sequenceNumber);
      })

      it("if the slice is equual to the element", () => {
        queryState.shrink({
          timestamp: messageIds[0].timestamp,
          sequenceNumber: messageIds[0].sequenceNumber,
        });

        expect(queryState.min).toBeUndefined();
        expect(queryState.max).toBeUndefined();
      })

      it("if the slice is greater than the element", () => {
        queryState.shrink({
          timestamp: messageIds[0].timestamp + 1
        });

        expect(queryState.min).toBeUndefined();
        expect(queryState.max).toBeUndefined();
      })

    });

    describe("2 elements", () => {
      beforeEach(() => {
        queryState.addMessageId(messageIds[0]);
        queryState.addMessageId(messageIds[1]);
      });

      it("if the slice is less than the 1st element", () => {
        queryState.shrink({
          timestamp: messageIds[0].timestamp - 1,
          sequenceNumber: 0,
        });

        expect(queryState.min?.timestamp).toEqual(messageIds[0].timestamp);
        expect(queryState.min?.sequenceNumber).toEqual(messageIds[0].sequenceNumber);
        expect(queryState.max?.timestamp).toEqual(messageIds[1].timestamp);
        expect(queryState.max?.sequenceNumber).toEqual(messageIds[1].sequenceNumber);
      })

      it("if the slice is equual to the 1st element", () => {
        queryState.shrink({
          timestamp: messageIds[0].timestamp,
          sequenceNumber: messageIds[0].sequenceNumber,
        });

        expect(queryState.min?.timestamp).toEqual(messageIds[1].timestamp);
        expect(queryState.min?.sequenceNumber).toEqual(messageIds[1].sequenceNumber);
        expect(queryState.max?.timestamp).toEqual(messageIds[1].timestamp);
        expect(queryState.max?.sequenceNumber).toEqual(messageIds[1].sequenceNumber);
      })

      it("if the slice is equual to the 2nd element", () => {
        queryState.shrink({
          timestamp: messageIds[1].timestamp,
          sequenceNumber: messageIds[1].sequenceNumber,
        });

        expect(queryState.min).toBeUndefined();
        expect(queryState.max).toBeUndefined();
      })

      it("if the slice is greater than the 2nd element", () => {
        queryState.shrink({
          timestamp: messageIds[1].timestamp + 1
        });

        expect(queryState.min).toBeUndefined();
        expect(queryState.max).toBeUndefined();
      })
    });

    describe("3 elements", () => {
      beforeEach(() => {
        queryState.addMessageId(messageIds[0]);
        queryState.addMessageId(messageIds[1]);
        queryState.addMessageId(messageIds[2]);
      });

      it("if the slice is less than the 1st element", () => {
        queryState.shrink({
          timestamp: messageIds[0].timestamp - 1,
          sequenceNumber: 0,
        });

        expect(queryState.min?.timestamp).toEqual(messageIds[0].timestamp);
        expect(queryState.min?.sequenceNumber).toEqual(messageIds[0].sequenceNumber);
        expect(queryState.max?.timestamp).toEqual(messageIds[2].timestamp);
        expect(queryState.max?.sequenceNumber).toEqual(messageIds[2].sequenceNumber);
      })

      it("if the slice is equual to the 1st element", () => {
        queryState.shrink({
          timestamp: messageIds[0].timestamp,
          sequenceNumber: messageIds[0].sequenceNumber,
        });

        expect(queryState.min?.timestamp).toEqual(messageIds[1].timestamp);
        expect(queryState.min?.sequenceNumber).toEqual(messageIds[1].sequenceNumber);
        expect(queryState.max?.timestamp).toEqual(messageIds[2].timestamp);
        expect(queryState.max?.sequenceNumber).toEqual(messageIds[2].sequenceNumber);
      })

      it("if the slice is between the 1st and the 2nd elements", () => {
        queryState.shrink({
          timestamp: messageIds[0].timestamp,
          sequenceNumber: messageIds[0].sequenceNumber + 1,
        });

        expect(queryState.min?.timestamp).toEqual(messageIds[1].timestamp);
        expect(queryState.min?.sequenceNumber).toEqual(messageIds[1].sequenceNumber);
        expect(queryState.max?.timestamp).toEqual(messageIds[2].timestamp);
        expect(queryState.max?.sequenceNumber).toEqual(messageIds[2].sequenceNumber);
      })

      it("if the slice is equual to the 2nd element", () => {
        queryState.shrink({
          timestamp: messageIds[1].timestamp,
          sequenceNumber: messageIds[1].sequenceNumber,
        });

        expect(queryState.min?.timestamp).toEqual(messageIds[2].timestamp);
        expect(queryState.min?.sequenceNumber).toEqual(messageIds[2].sequenceNumber);
        expect(queryState.max?.timestamp).toEqual(messageIds[2].timestamp);
        expect(queryState.max?.sequenceNumber).toEqual(messageIds[2].sequenceNumber);
      })

      it("if the slice is between the 2nd and the 3d elements", () => {
        queryState.shrink({
          timestamp: messageIds[1].timestamp + 1,
          sequenceNumber: messageIds[1].sequenceNumber,
        });

        expect(queryState.min?.timestamp).toEqual(messageIds[2].timestamp);
        expect(queryState.min?.sequenceNumber).toEqual(messageIds[2].sequenceNumber);
        expect(queryState.max?.timestamp).toEqual(messageIds[2].timestamp);
        expect(queryState.max?.sequenceNumber).toEqual(messageIds[2].sequenceNumber);
      })

      it("if the slice is greater than the 3rd element", () => {
        queryState.shrink({
          timestamp: messageIds[2].timestamp + 1
        });

        expect(queryState.min).toBeUndefined();
        expect(queryState.max).toBeUndefined();
      })
    });
  });
});
