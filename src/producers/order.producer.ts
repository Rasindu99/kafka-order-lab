import { kafka } from "../config/kafka";
import { TOPICS } from "../topics/topic.names";
import { BaseEvent, OrderCreatedPayload } from "../utils/event.factory";

// we arent returning anything, just publishing to kafka, so voide.g. Promise<void>
export async function publishOrderCreated(
  event: BaseEvent<OrderCreatedPayload>
): Promise<void> {
  const producer = kafka.producer();

  try {
    console.log("Connecting order producer...");
    await producer.connect();
    console.log("Order producer connected.");

    const result = await producer.send({
      topic: TOPICS.ORDER_CREATED,
      messages: [
        {
          key: event.key,
          value: JSON.stringify(event),
        },
      ],
    });

    console.log("order.created event published successfully.");
    console.log("Kafka result:", result);
  } catch (error) {
    console.error("Failed to publish order.created event:", error);
    throw error;
  } finally {
    await producer.disconnect();
    console.log("Order producer disconnected.");
  }
}