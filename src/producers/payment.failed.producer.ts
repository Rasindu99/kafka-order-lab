import { kafka } from "../config/kafka";
import { TOPICS } from "../topics/topic.names";
import { BaseEvent, PaymentFailedPayload } from "../utils/event.factory";

export async function publishPaymentFailed(
  event: BaseEvent<PaymentFailedPayload>
): Promise<void> {
  const producer = kafka.producer();

  try {
    console.log("Connecting payment.failed producer...");
    await producer.connect();
    console.log("Payment.failed producer connected.");

    const result = await producer.send({
      topic: TOPICS.PAYMENT_FAILED,
      messages: [
        {
          key: event.key,
          value: JSON.stringify(event),
        },
      ],
    });

    console.log("payment.failed event published successfully.");
    console.log("Kafka result:", result);
  } catch (error) {
    console.error("Failed to publish payment.failed event:", error);
    throw error;
  } finally {
    await producer.disconnect();
    console.log("Payment.failed producer disconnected.");
  }
}