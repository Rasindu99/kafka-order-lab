import { kafka } from "../config/kafka";
import { TOPICS } from "../topics/topic.names";

async function runConsumer() {
  const consumer = kafka.consumer({
    groupId: "order-created-consumer-group",
  });

  try {
    console.log("Connecting order.created consumer...");
    await consumer.connect();
    console.log("Order.created consumer connected.");

    await consumer.subscribe({
      topic: TOPICS.ORDER_CREATED,
      fromBeginning: true,
    });

    console.log(`Subscribed to topic "${TOPICS.ORDER_CREATED}".`);
    console.log("Waiting for order.created messages...\n");

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const rawValue = message.value?.toString() ?? "";
        const parsedValue = rawValue ? JSON.parse(rawValue) : null;

        console.log("Received order.created message:");
        console.log({
          topic,
          partition,
          offset: message.offset,
          key: message.key?.toString(),
          value: parsedValue,
          timestamp: message.timestamp,
        });
        console.log("--------------------------------------------------");
      },
    });
  } catch (error) {
    console.error("order.created consumer failed:", error);
    await consumer.disconnect();
  }
}

runConsumer();