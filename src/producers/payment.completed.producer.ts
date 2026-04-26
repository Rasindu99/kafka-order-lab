import { kafka } from "../config/kafka";
import { TOPICS } from "../topics/topic.names";
import { BaseEvent, PaymentCompletedPayload } from "../utils/event.factory";

export async function publishPaymentCompleted(
  event: BaseEvent<PaymentCompletedPayload>
): Promise<void> {
  const producer = kafka.producer();

  try {
    console.log("Connecting payment.completed producer...");
    await producer.connect();
    console.log("Payment.completed producer connected.");

    const result = await producer.send({
      topic: TOPICS.PAYMENT_COMPLETED,
      messages: [
        {
          key: event.key,
          value: JSON.stringify(event),
        },
      ],
    });

    console.log("payment.completed event published successfully.");
    console.log("Kafka result:", result);
  } catch (error) {
    console.error("Failed to publish payment.completed event:", error);
    throw error;
  } finally {
    await producer.disconnect();
    console.log("Payment.completed producer disconnected.");
  }
}