import { kafka } from "../config/kafka";
import { TOPICS } from "../topics/topic.names";
import {
  BaseEvent,
  PaymentCompletedPayload,
  createNotificationSendEvent,
} from "../utils/event.factory";
import { publishNotificationSend } from "../producers/notification.producer";

async function runConsumer() {
  const consumer = kafka.consumer({
    groupId: "notification-consumer-group",
  });

  try {
    console.log("Connecting notification consumer...");
    await consumer.connect();
    console.log("Notification consumer connected.");

    await consumer.subscribe({
      topic: TOPICS.PAYMENT_COMPLETED,
      fromBeginning: true,
    });

    console.log(`Subscribed to topic "${TOPICS.PAYMENT_COMPLETED}".`);
    console.log("Waiting for payment.completed messages in notification...\n");

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const rawValue = message.value?.toString() ?? "";

        if (!rawValue) {
          console.log("Skipping empty notification message.");
          return;
        }

        const parsedValue = JSON.parse(
          rawValue
        ) as BaseEvent<PaymentCompletedPayload>;

        console.log("Notification consumer received payment.completed:");
        console.log({
          topic,
          partition,
          offset: message.offset,
          key: message.key?.toString(),
          value: parsedValue,
          timestamp: message.timestamp,
        });

        const notificationEvent = createNotificationSendEvent({
          orderId: parsedValue.payload.orderId,
          customerId: parsedValue.payload.customerId,
          amount: parsedValue.payload.amount,
          paymentId: parsedValue.payload.paymentId,
          channel: "EMAIL",
          message: `Payment received successfully for order ${parsedValue.payload.orderId}. Amount: $${parsedValue.payload.amount}`,
        });

        await publishNotificationSend(notificationEvent);

        console.log("Produced notification.send event:");
        console.log(notificationEvent);
        console.log("--------------------------------------------------");
      },
    });
  } catch (error) {
    console.error("notification consumer failed:", error);
    await consumer.disconnect();
  }
}

runConsumer();