export type OrderCreatedPayload = {
  orderId: string;
  customerId: string;
  amount: number;
  items: Array<{
    sku: string;
    quantity: number;
  }>;
};

// Generic <TPayload> Used to make a type reusable.

export type BaseEvent<TPayload> = {
  eventId: string;
  eventType: string;
  eventVersion: number;
  occurredAt: string;
  source: string;
  key: string;
  payload: TPayload;
};

export function createOrderCreatedEvent(
  payload: OrderCreatedPayload
): BaseEvent<OrderCreatedPayload> {
  return {
    eventId: `evt-${Date.now()}-${Math.floor(Math.random() * 100000)}`,
    eventType: "order.created",
    eventVersion: 1,
    occurredAt: new Date().toISOString(),
    source: "order-api",
    key: payload.customerId,
    payload,
  };
}