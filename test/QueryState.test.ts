import { MessageRef } from "@streamr/protocol";
import { QueryState } from "../src/QueryState";
import { mockStreamMessageRange } from "./test-utils";

const messageRefs = Array.from(mockStreamMessageRange(100200300, 100200309))
  .map(m => m.messageId.toMessageRef());

describe("QueryState", () => {

  describe("calculates min and max for a set of", () => {
    let queryState: QueryState;

    beforeEach(() => {
      queryState = new QueryState();
    });

    it("1 MessageID", () => {
      queryState.addResponseMessageRef(messageRefs[0]);

      expect(queryState.min?.timestamp).toEqual(messageRefs[0].timestamp);
      expect(queryState.min?.sequenceNumber).toEqual(messageRefs[0].sequenceNumber);
      expect(queryState.max?.timestamp).toEqual(messageRefs[0].timestamp);
      expect(queryState.max?.sequenceNumber).toEqual(messageRefs[0].sequenceNumber);
    });

    it("2 MessageIDs", () => {
      queryState.addResponseMessageRef(messageRefs[0]);
      queryState.addResponseMessageRef(messageRefs[1]);

      expect(queryState.min?.timestamp).toEqual(messageRefs[0].timestamp);
      expect(queryState.min?.sequenceNumber).toEqual(messageRefs[0].sequenceNumber);
      expect(queryState.max?.timestamp).toEqual(messageRefs[1].timestamp);
      expect(queryState.max?.sequenceNumber).toEqual(messageRefs[1].sequenceNumber);
    });

    it("3 MessageIDs", () => {
      queryState.addResponseMessageRef(messageRefs[0]);
      queryState.addResponseMessageRef(messageRefs[1]);
      queryState.addResponseMessageRef(messageRefs[2]);

      expect(queryState.min?.timestamp).toEqual(messageRefs[0].timestamp);
      expect(queryState.min?.sequenceNumber).toEqual(messageRefs[0].sequenceNumber);
      expect(queryState.max?.timestamp).toEqual(messageRefs[2].timestamp);
      expect(queryState.max?.sequenceNumber).toEqual(messageRefs[2].sequenceNumber);
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
      queryStateA.addResponseMessageRef(messageRefs[0]);
      queryStateA.addResponseMessageRef(messageRefs[1]);

      const result = Array.from(queryStateA.subtract(queryStateB));
      expect(result).toHaveLength(2);
      expect(result).toEqual([messageRefs[0], messageRefs[1]]);
    })

    test("filled from empty", () => {
      queryStateB.addResponseMessageRef(messageRefs[0]);
      queryStateB.addResponseMessageRef(messageRefs[1]);

      const result = Array.from(queryStateA.subtract(queryStateB));
      expect(result).toHaveLength(0);
    })

    test("filled from filled if elements are the same", () => {
      queryStateA.addResponseMessageRef(messageRefs[0]);
      queryStateA.addResponseMessageRef(messageRefs[1]);

      queryStateB.addResponseMessageRef(messageRefs[0]);
      queryStateB.addResponseMessageRef(messageRefs[1]);

      const result = Array.from(queryStateA.subtract(queryStateB));
      expect(result).toHaveLength(0);
    })

    test("filled from filled if elements are not the same", () => {
      queryStateA.addResponseMessageRef(messageRefs[0]);
      queryStateA.addResponseMessageRef(messageRefs[1]);

      queryStateB.addResponseMessageRef(messageRefs[2]);
      queryStateB.addResponseMessageRef(messageRefs[3]);

      const result = Array.from(queryStateA.subtract(queryStateB));
      expect(result).toHaveLength(2);
      expect(result).toEqual([messageRefs[0], messageRefs[1]]);
    })
  });

  describe("shrinks a set of", () => {
    let queryState: QueryState;

    beforeEach(() => {
      queryState = new QueryState();
    });

    describe("1 element", () => {
      beforeEach(() => {
        queryState.addResponseMessageRef(messageRefs[0]);
      });

      it("if the slice is less than the element", () => {
        queryState.shrink(new MessageRef(messageRefs[0].timestamp - 1, 0));

        expect(queryState.min?.timestamp).toEqual(messageRefs[0].timestamp);
        expect(queryState.min?.sequenceNumber).toEqual(messageRefs[0].sequenceNumber);
        expect(queryState.max?.timestamp).toEqual(messageRefs[0].timestamp);
        expect(queryState.max?.sequenceNumber).toEqual(messageRefs[0].sequenceNumber);
      })

      it("if the slice is equual to the element", () => {
        queryState.shrink(new MessageRef(messageRefs[0].timestamp, messageRefs[0].sequenceNumber));

        expect(queryState.min).toBeUndefined();
        expect(queryState.max).toBeUndefined();
      })

      it("if the slice is greater than the element", () => {
        queryState.shrink(new MessageRef(messageRefs[0].timestamp + 1, 0));

        expect(queryState.min).toBeUndefined();
        expect(queryState.max).toBeUndefined();
      })

    });

    describe("2 elements", () => {
      beforeEach(() => {
        queryState.addResponseMessageRef(messageRefs[0]);
        queryState.addResponseMessageRef(messageRefs[1]);
      });

      it("if the slice is less than the 1st element", () => {
        queryState.shrink(new MessageRef(messageRefs[0].timestamp - 1, 0));

        expect(queryState.min?.timestamp).toEqual(messageRefs[0].timestamp);
        expect(queryState.min?.sequenceNumber).toEqual(messageRefs[0].sequenceNumber);
        expect(queryState.max?.timestamp).toEqual(messageRefs[1].timestamp);
        expect(queryState.max?.sequenceNumber).toEqual(messageRefs[1].sequenceNumber);
      })

      it("if the slice is equual to the 1st element", () => {
        queryState.shrink(new MessageRef(messageRefs[0].timestamp, messageRefs[0].sequenceNumber));

        expect(queryState.min?.timestamp).toEqual(messageRefs[1].timestamp);
        expect(queryState.min?.sequenceNumber).toEqual(messageRefs[1].sequenceNumber);
        expect(queryState.max?.timestamp).toEqual(messageRefs[1].timestamp);
        expect(queryState.max?.sequenceNumber).toEqual(messageRefs[1].sequenceNumber);
      })

      it("if the slice is equual to the 2nd element", () => {
        queryState.shrink(new MessageRef(messageRefs[1].timestamp, messageRefs[1].sequenceNumber));

        expect(queryState.min).toBeUndefined();
        expect(queryState.max).toBeUndefined();
      })

      it("if the slice is greater than the 2nd element", () => {
        queryState.shrink(new MessageRef(messageRefs[1].timestamp + 1, 0));

        expect(queryState.min).toBeUndefined();
        expect(queryState.max).toBeUndefined();
      })
    });

    describe("3 elements", () => {
      beforeEach(() => {
        queryState.addResponseMessageRef(messageRefs[0]);
        queryState.addResponseMessageRef(messageRefs[1]);
        queryState.addResponseMessageRef(messageRefs[2]);
      });

      it("if the slice is less than the 1st element", () => {
        queryState.shrink(new MessageRef(messageRefs[0].timestamp - 1, 0));

        expect(queryState.min?.timestamp).toEqual(messageRefs[0].timestamp);
        expect(queryState.min?.sequenceNumber).toEqual(messageRefs[0].sequenceNumber);
        expect(queryState.max?.timestamp).toEqual(messageRefs[2].timestamp);
        expect(queryState.max?.sequenceNumber).toEqual(messageRefs[2].sequenceNumber);
      })

      it("if the slice is equual to the 1st element", () => {
        queryState.shrink(new MessageRef(messageRefs[0].timestamp, messageRefs[0].sequenceNumber));

        expect(queryState.min?.timestamp).toEqual(messageRefs[1].timestamp);
        expect(queryState.min?.sequenceNumber).toEqual(messageRefs[1].sequenceNumber);
        expect(queryState.max?.timestamp).toEqual(messageRefs[2].timestamp);
        expect(queryState.max?.sequenceNumber).toEqual(messageRefs[2].sequenceNumber);
      })

      it("if the slice is between the 1st and the 2nd elements", () => {
        queryState.shrink(new MessageRef(messageRefs[0].timestamp, messageRefs[0].sequenceNumber + 1));

        expect(queryState.min?.timestamp).toEqual(messageRefs[1].timestamp);
        expect(queryState.min?.sequenceNumber).toEqual(messageRefs[1].sequenceNumber);
        expect(queryState.max?.timestamp).toEqual(messageRefs[2].timestamp);
        expect(queryState.max?.sequenceNumber).toEqual(messageRefs[2].sequenceNumber);
      })

      it("if the slice is equual to the 2nd element", () => {
        queryState.shrink(new MessageRef(messageRefs[1].timestamp, messageRefs[1].sequenceNumber));

        expect(queryState.min?.timestamp).toEqual(messageRefs[2].timestamp);
        expect(queryState.min?.sequenceNumber).toEqual(messageRefs[2].sequenceNumber);
        expect(queryState.max?.timestamp).toEqual(messageRefs[2].timestamp);
        expect(queryState.max?.sequenceNumber).toEqual(messageRefs[2].sequenceNumber);
      })

      it("if the slice is between the 2nd and the 3d elements", () => {
        queryState.shrink(new MessageRef(messageRefs[1].timestamp, messageRefs[1].sequenceNumber + 1));

        expect(queryState.min?.timestamp).toEqual(messageRefs[2].timestamp);
        expect(queryState.min?.sequenceNumber).toEqual(messageRefs[2].sequenceNumber);
        expect(queryState.max?.timestamp).toEqual(messageRefs[2].timestamp);
        expect(queryState.max?.sequenceNumber).toEqual(messageRefs[2].sequenceNumber);
      })

      it("if the slice is greater than the 3rd element", () => {
        queryState.shrink(new MessageRef(messageRefs[2].timestamp + 1, 0));

        expect(queryState.min).toBeUndefined();
        expect(queryState.max).toBeUndefined();
      })
    });
  });
});
