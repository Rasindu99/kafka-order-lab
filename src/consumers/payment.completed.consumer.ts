import { kafka } from "../config/kafka";
import { TOPICS } from "../topics/topic.names";

async function runConsumer() {
  const consumer = kafka.consumer({
    groupId: "payment-completed-consumer-group",
  });

  try {
    console.log("Connecting payment.completed consumer...");
    await consumer.connect();
    console.log("Payment.completed consumer connected.");

    await consumer.subscribe({
      topic: TOPICS.PAYMENT_COMPLETED,
      fromBeginning: true,
    });

    console.log(`Subscribed to topic "${TOPICS.PAYMENT_COMPLETED}".`);
    console.log("Waiting for payment.completed messages...\n");

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const rawValue = message.value?.toString() ?? "";
        const parsedValue = rawValue ? JSON.parse(rawValue) : null;

        console.log("Received payment.completed message:");
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
    console.error("payment.completed consumer failed:", error);
    await consumer.disconnect();
  }
}

runConsumer();