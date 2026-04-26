import { kafka } from "../config/kafka";
import { TOPICS } from "../topics/topic.names";
import { BaseEvent, NotificationSendPayload } from "../utils/event.factory";

export async function publishNotificationSend(
  event: BaseEvent<NotificationSendPayload>
): Promise<void> {
  const producer = kafka.producer();

  try {
    console.log("Connecting notification producer...");
    await producer.connect();
    console.log("Notification producer connected.");

    const result = await producer.send({
      topic: TOPICS.NOTIFICATION_SEND,
      messages: [
        {
          key: event.key,
          value: JSON.stringify(event),
        },
      ],
    });

    console.log("notification.send event published successfully.");
    console.log("Kafka result:", result);
  } catch (error) {
    console.error("Failed to publish notification.send event:", error);
    throw error;
  } finally {
    await producer.disconnect();
    console.log("Notification producer disconnected.");
  }
}