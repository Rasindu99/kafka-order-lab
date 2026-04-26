import { kafka } from "../config/kafka";
import { TOPICS } from "../topics/topic.names";

async function runConsumer() {
  const consumer = kafka.consumer({
    groupId: "payment-failed-consumer-group",
  });

  try {
    console.log("Connecting payment.failed consumer...");
    await consumer.connect();
    console.log("Payment.failed consumer connected.");

    await consumer.subscribe({
      topic: TOPICS.PAYMENT_FAILED,
      fromBeginning: true,
    });

    console.log(`Subscribed to topic "${TOPICS.PAYMENT_FAILED}".`);
    console.log("Waiting for payment.failed messages...\n");

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const rawValue = message.value?.toString() ?? "";
        const parsedValue = rawValue ? JSON.parse(rawValue) : null;

        console.log("Received payment.failed message:");
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
    console.error("payment.failed consumer failed:", error);
    await consumer.disconnect();
  }
}

runConsumer();