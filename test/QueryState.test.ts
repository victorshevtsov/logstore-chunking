import { StreamMessage } from "@streamr/protocol";
import data from "../data/data_5.json";
import { QueryState } from "../src/QueryState";

const messageIds = data
  .map(StreamMessage.deserialize)
  .map(m => m.messageId);

describe("QueryState", () => {

  describe("calculates min and max for a set of", () => {
    let queryState: QueryState;

    beforeEach(() => {
      queryState = new QueryState();
    });

    it("1 MessageID", () => {
      queryState.addResponseMessageId(messageIds[0]);

      expect(queryState.min?.timestamp).toEqual(messageIds[0].timestamp);
      expect(queryState.min?.sequenceNumber).toEqual(messageIds[0].sequenceNumber);
      expect(queryState.max?.timestamp).toEqual(messageIds[0].timestamp);
      expect(queryState.max?.sequenceNumber).toEqual(messageIds[0].sequenceNumber);
    });

    it("2 MessageIDs", () => {
      queryState.addResponseMessageId(messageIds[0]);
      queryState.addResponseMessageId(messageIds[1]);

      expect(queryState.min?.timestamp).toEqual(messageIds[0].timestamp);
      expect(queryState.min?.sequenceNumber).toEqual(messageIds[0].sequenceNumber);
      expect(queryState.max?.timestamp).toEqual(messageIds[1].timestamp);
      expect(queryState.max?.sequenceNumber).toEqual(messageIds[1].sequenceNumber);
    });

    it("3 MessageIDs", () => {
      queryState.addResponseMessageId(messageIds[0]);
      queryState.addResponseMessageId(messageIds[1]);
      queryState.addResponseMessageId(messageIds[2]);

      expect(queryState.min?.timestamp).toEqual(messageIds[0].timestamp);
      expect(queryState.min?.sequenceNumber).toEqual(messageIds[0].sequenceNumber);
      expect(queryState.max?.timestamp).toEqual(messageIds[2].timestamp);
      expect(queryState.max?.sequenceNumber).toEqual(messageIds[2].sequenceNumber);
    });
  });

  describe("subtracts", () => {
    let queryStateA: QueryState;
    let queryStateB: QueryState;

    beforeEach(() => {
      queryStateA = new QueryState();
      queryStateB = new QueryState();
    });

    test("empty from empty", () => {
      const result = Array.from(queryStateA.subtract(queryStateB));
      expect(result).toHaveLength(0);
    })

    test("empty from filled", () => {
      queryStateA.addResponseMessageId(messageIds[0]);
      queryStateA.addResponseMessageId(messageIds[1]);

      const result = Array.from(queryStateA.subtract(queryStateB));
      expect(result).toHaveLength(2);
      expect(result).toEqual([messageIds[0], messageIds[1]]);
    })

    test("filled from empty", () => {
      queryStateB.addResponseMessageId(messageIds[0]);
      queryStateB.addResponseMessageId(messageIds[1]);

      const result = Array.from(queryStateA.subtract(queryStateB));
      expect(result).toHaveLength(0);
    })

    test("filled from filled if elements are the same", () => {
      queryStateA.addResponseMessageId(messageIds[0]);
      queryStateA.addResponseMessageId(messageIds[1]);

      queryStateB.addResponseMessageId(messageIds[0]);
      queryStateB.addResponseMessageId(messageIds[1]);

      const result = Array.from(queryStateA.subtract(queryStateB));
      expect(result).toHaveLength(0);
    })

    test("filled from filled if elements are not the same", () => {
      queryStateA.addResponseMessageId(messageIds[0]);
      queryStateA.addResponseMessageId(messageIds[1]);

      queryStateB.addResponseMessageId(messageIds[2]);
      queryStateB.addResponseMessageId(messageIds[3]);

      const result = Array.from(queryStateA.subtract(queryStateB));
      expect(result).toHaveLength(2);
      expect(result).toEqual([messageIds[0], messageIds[1]]);
    })
  });

  describe("shrinks a set of", () => {
    let queryState: QueryState;

    beforeEach(() => {
      queryState = new QueryState();
    });

    describe("1 element", () => {
      beforeEach(() => {
        queryState.addResponseMessageId(messageIds[0]);
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
        queryState.addResponseMessageId(messageIds[0]);
        queryState.addResponseMessageId(messageIds[1]);
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
        queryState.addResponseMessageId(messageIds[0]);
        queryState.addResponseMessageId(messageIds[1]);
        queryState.addResponseMessageId(messageIds[2]);
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
