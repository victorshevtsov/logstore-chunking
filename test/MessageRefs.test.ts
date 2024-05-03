import { MessageRef } from "@streamr/protocol";
import { MessageRefs } from "../src/MessageRefs";
import { mockStreamMessageRange } from "./test-utils";

const data = Array
  .from(mockStreamMessageRange(100200300, 100200309))
  .map(m => m.messageId.toMessageRef());

describe("MessageRefs", () => {

  describe("calculates min and max for", () => {
    let messageRefs: MessageRefs;

    beforeEach(() => {
      messageRefs = new MessageRefs();
    });

    it("an empty set", () => {
      expect(messageRefs.min).toBeUndefined();
      expect(messageRefs.max).toBeUndefined();
    });

    describe("a set of", () => {

      it("1 MessageRef", () => {
        messageRefs.push(data[0]);

        expect(messageRefs.min?.timestamp).toEqual(data[0].timestamp);
        expect(messageRefs.min?.sequenceNumber).toEqual(data[0].sequenceNumber);
        expect(messageRefs.max?.timestamp).toEqual(data[0].timestamp);
        expect(messageRefs.max?.sequenceNumber).toEqual(data[0].sequenceNumber);
      });

      it("2 MessageRefs", () => {
        messageRefs.push(data[0]);
        messageRefs.push(data[1]);

        expect(messageRefs.min?.timestamp).toEqual(data[0].timestamp);
        expect(messageRefs.min?.sequenceNumber).toEqual(data[0].sequenceNumber);
        expect(messageRefs.max?.timestamp).toEqual(data[1].timestamp);
        expect(messageRefs.max?.sequenceNumber).toEqual(data[1].sequenceNumber);
      });

      it("3 MessageRefs", () => {
        messageRefs.push(data[0]);
        messageRefs.push(data[1]);
        messageRefs.push(data[2]);

        expect(messageRefs.min?.timestamp).toEqual(data[0].timestamp);
        expect(messageRefs.min?.sequenceNumber).toEqual(data[0].sequenceNumber);
        expect(messageRefs.max?.timestamp).toEqual(data[2].timestamp);
        expect(messageRefs.max?.sequenceNumber).toEqual(data[2].sequenceNumber);
      });
    });
  });

  describe("subtracts", () => {
    let messageRefsA: MessageRefs;
    let messageRefsB: MessageRefs;

    beforeEach(() => {
      messageRefsA = new MessageRefs();
      messageRefsB = new MessageRefs();
    });

    it("empty from empty", () => {
      const result = Array.from(messageRefsA.subtract(messageRefsB));
      expect(result).toHaveLength(0);
    })

    it("empty from filled", () => {
      messageRefsA.push(data[0]);
      messageRefsA.push(data[1]);

      const result = Array.from(messageRefsA.subtract(messageRefsB));
      expect(result).toHaveLength(2);
      expect(result).toEqual([data[0], data[1]]);
    })

    it("filled from empty", () => {
      messageRefsB.push(data[0]);
      messageRefsB.push(data[1]);

      const result = Array.from(messageRefsA.subtract(messageRefsB));
      expect(result).toHaveLength(0);
    })

    it("filled from filled if elements are the same", () => {
      messageRefsA.push(data[0]);
      messageRefsA.push(data[1]);

      messageRefsB.push(data[0]);
      messageRefsB.push(data[1]);

      const result = Array.from(messageRefsA.subtract(messageRefsB));
      expect(result).toHaveLength(0);
    })

    it("filled from filled if elements are not the same", () => {
      messageRefsA.push(data[0]);
      messageRefsA.push(data[1]);

      messageRefsB.push(data[2]);
      messageRefsB.push(data[3]);

      const result = Array.from(messageRefsA.subtract(messageRefsB));
      expect(result).toHaveLength(2);
      expect(result).toEqual([data[0], data[1]]);
    })
  });

  describe("shrinks a set of", () => {
    let messageRefs: MessageRefs;

    beforeEach(() => {
      messageRefs = new MessageRefs();
    });

    describe("1 element", () => {
      beforeEach(() => {
        messageRefs.push(data[0]);
      });

      it("if the slice is less than the element", () => {
        messageRefs.shrink(new MessageRef(data[0].timestamp - 1, 0));

        expect(messageRefs.min?.timestamp).toEqual(data[0].timestamp);
        expect(messageRefs.min?.sequenceNumber).toEqual(data[0].sequenceNumber);
        expect(messageRefs.max?.timestamp).toEqual(data[0].timestamp);
        expect(messageRefs.max?.sequenceNumber).toEqual(data[0].sequenceNumber);
      })

      it("if the slice is equal to the element", () => {
        messageRefs.shrink(new MessageRef(data[0].timestamp, data[0].sequenceNumber));

        expect(messageRefs.min).toBeUndefined();
        expect(messageRefs.max).toBeUndefined();
      })

      it("if the slice is greater than the element", () => {
        messageRefs.shrink(new MessageRef(data[0].timestamp + 1, 0));

        expect(messageRefs.min).toBeUndefined();
        expect(messageRefs.max).toBeUndefined();
      })

    });

    describe("2 elements", () => {
      beforeEach(() => {
        messageRefs.push(data[0]);
        messageRefs.push(data[1]);
      });

      it("if the slice is less than the 1st element", () => {
        messageRefs.shrink(new MessageRef(data[0].timestamp - 1, 0));

        expect(messageRefs.min?.timestamp).toEqual(data[0].timestamp);
        expect(messageRefs.min?.sequenceNumber).toEqual(data[0].sequenceNumber);
        expect(messageRefs.max?.timestamp).toEqual(data[1].timestamp);
        expect(messageRefs.max?.sequenceNumber).toEqual(data[1].sequenceNumber);
      })

      it("if the slice is equal to the 1st element", () => {
        messageRefs.shrink(new MessageRef(data[0].timestamp, data[0].sequenceNumber));

        expect(messageRefs.min?.timestamp).toEqual(data[1].timestamp);
        expect(messageRefs.min?.sequenceNumber).toEqual(data[1].sequenceNumber);
        expect(messageRefs.max?.timestamp).toEqual(data[1].timestamp);
        expect(messageRefs.max?.sequenceNumber).toEqual(data[1].sequenceNumber);
      })

      it("if the slice is equal to the 2nd element", () => {
        messageRefs.shrink(new MessageRef(data[1].timestamp, data[1].sequenceNumber));

        expect(messageRefs.min).toBeUndefined();
        expect(messageRefs.max).toBeUndefined();
      })

      it("if the slice is greater than the 2nd element", () => {
        messageRefs.shrink(new MessageRef(data[1].timestamp + 1, 0));

        expect(messageRefs.min).toBeUndefined();
        expect(messageRefs.max).toBeUndefined();
      })
    });

    describe("3 elements", () => {
      beforeEach(() => {
        messageRefs.push(data[0]);
        messageRefs.push(data[1]);
        messageRefs.push(data[2]);
      });

      it("if the slice is less than the 1st element", () => {
        messageRefs.shrink(new MessageRef(data[0].timestamp - 1, 0));

        expect(messageRefs.min?.timestamp).toEqual(data[0].timestamp);
        expect(messageRefs.min?.sequenceNumber).toEqual(data[0].sequenceNumber);
        expect(messageRefs.max?.timestamp).toEqual(data[2].timestamp);
        expect(messageRefs.max?.sequenceNumber).toEqual(data[2].sequenceNumber);
      })

      it("if the slice is equal to the 1st element", () => {
        messageRefs.shrink(new MessageRef(data[0].timestamp, data[0].sequenceNumber));

        expect(messageRefs.min?.timestamp).toEqual(data[1].timestamp);
        expect(messageRefs.min?.sequenceNumber).toEqual(data[1].sequenceNumber);
        expect(messageRefs.max?.timestamp).toEqual(data[2].timestamp);
        expect(messageRefs.max?.sequenceNumber).toEqual(data[2].sequenceNumber);
      })

      it("if the slice is between the 1st and the 2nd elements", () => {
        messageRefs.shrink(new MessageRef(data[0].timestamp, data[0].sequenceNumber + 1));

        expect(messageRefs.min?.timestamp).toEqual(data[1].timestamp);
        expect(messageRefs.min?.sequenceNumber).toEqual(data[1].sequenceNumber);
        expect(messageRefs.max?.timestamp).toEqual(data[2].timestamp);
        expect(messageRefs.max?.sequenceNumber).toEqual(data[2].sequenceNumber);
      })

      it("if the slice is equal to the 2nd element", () => {
        messageRefs.shrink(new MessageRef(data[1].timestamp, data[1].sequenceNumber));

        expect(messageRefs.min?.timestamp).toEqual(data[2].timestamp);
        expect(messageRefs.min?.sequenceNumber).toEqual(data[2].sequenceNumber);
        expect(messageRefs.max?.timestamp).toEqual(data[2].timestamp);
        expect(messageRefs.max?.sequenceNumber).toEqual(data[2].sequenceNumber);
      })

      it("if the slice is between the 2nd and the 3d elements", () => {
        messageRefs.shrink(new MessageRef(data[1].timestamp, data[1].sequenceNumber + 1));

        expect(messageRefs.min?.timestamp).toEqual(data[2].timestamp);
        expect(messageRefs.min?.sequenceNumber).toEqual(data[2].sequenceNumber);
        expect(messageRefs.max?.timestamp).toEqual(data[2].timestamp);
        expect(messageRefs.max?.sequenceNumber).toEqual(data[2].sequenceNumber);
      })

      it("if the slice is greater than the 3rd element", () => {
        messageRefs.shrink(new MessageRef(data[2].timestamp + 1, 0));

        expect(messageRefs.min).toBeUndefined();
        expect(messageRefs.max).toBeUndefined();
      })
    });
  });
});
