import { Router, Request, Response } from "express";
import { createOrderCreatedEvent, OrderCreatedPayload } from "../../utils/event.factory";
import { publishOrderCreated } from "../../producers/order.producer";

const router = Router();

function isValidOrderPayload(body: unknown): body is OrderCreatedPayload {

  /*
   KNOWLEDGE CHECKPOINT: Unknown vs Any
    - `unknown` is a safer alternative to `any` 
    - It forces you to perform type checks before using the value, preventing potential runtime errors.
    - In contrast, `any` allows you to use the value without any checks
    - you just assume what is inside and use it without checking
  */

  /*
    - body is OrderCreatedPayload
    - means that if this function returns true, then TypeScript will treat `body` as an
      `OrderCreatedPayload` from that point onward.
    - In JS, you validate and then you personally know it is valid.
    - In TS, you validate and also tell the compiler .
  */

  if (!body || typeof body !== "object") {
    return false;
  }

  /*
    - candidate is Record<string, unknown>
    - means that For TypeScript purposes, treat this value as type Record<string, unknown> .
    - Record<string, unknown> is a TypeScript utility type that represents an object with string keys and 
      values of any type (unknown in this case).
    - This allows us to perform type checks on the properties of candidate without TypeScript throwing 
      errors about accessing properties on an unknown type.
    - with in function body is still unknown, 
    - if it returns true, TypeScript can treat the argument (body) as OrderCreatedPayload outside the 
      function, at the call site
  */

  const candidate = body as Record<string, unknown>;

  if (typeof candidate.orderId !== "string") return false;
  if (typeof candidate.customerId !== "string") return false;
  if (typeof candidate.amount !== "number") return false;
  if (!Array.isArray(candidate.items)) return false;

  for (const item of candidate.items) {
    if (!item || typeof item !== "object") return false;

    const itemRecord = item as Record<string, unknown>;
    if (typeof itemRecord.sku !== "string") return false;
    if (typeof itemRecord.quantity !== "number") return false;
  }

  return true;
}

router.post("/orders", async (req: Request, res: Response) => {
  try {
    if (!isValidOrderPayload(req.body)) {
      return res.status(400).json({
        success: false,
        message: "Invalid order payload.",
      });
    }

    const event = createOrderCreatedEvent(req.body);

    await publishOrderCreated(event);

    return res.status(201).json({
      success: true,
      message: "Order accepted and event published.",
      data: {
        orderId: req.body.orderId,
        eventId: event.eventId,
        topic: "order.created",
        key: event.key,
      },
    });
  } catch (error) {
    console.error("POST /orders failed:", error);

    return res.status(500).json({
      success: false,
      message: "Failed to publish order event.",
    });
  }
});

export default router;