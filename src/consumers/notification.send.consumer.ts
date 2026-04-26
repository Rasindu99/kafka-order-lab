import { kafka } from "../config/kafka";
import { TOPICS } from "../topics/topic.names";

async function runConsumer() {
  const consumer = kafka.consumer({
    groupId: "notification-send-consumer-group",
  });

  try {
    console.log("Connecting notification.send consumer...");
    await consumer.connect();
    console.log("Notification.send consumer connected.");

    await consumer.subscribe({
      topic: TOPICS.NOTIFICATION_SEND,
      fromBeginning: true,
    });

    console.log(`Subscribed to topic "${TOPICS.NOTIFICATION_SEND}".`);
    console.log("Waiting for notification.send messages...\n");

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const rawValue = message.value?.toString() ?? "";
        const parsedValue = rawValue ? JSON.parse(rawValue) : null;

        console.log("Received notification.send message:");
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
    console.error("notification.send consumer failed:", error);
    await consumer.disconnect();
  }
}

runConsumer();