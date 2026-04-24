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

    /*
      when we send events to kafka , events with same event-key goes same partition , but its not guarantee that events with different event-key will go different partition, it depends on the number of partitions and the hashing algorithm used by Kafka to determine the partition for a given key.

      - If the number of partitions is greater than the number of unique keys, then some keys will be 
        hashed to the same partition, resulting in multiple keys being stored in the same partition.

      - If the number of partitions is less than or equal to the number of unique keys, then each key will 
        be hashed to a different partition, ensuring that events with different keys are distributed across different partitions.        
    */

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