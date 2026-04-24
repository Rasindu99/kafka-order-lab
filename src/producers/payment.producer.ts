import { kafka } from "../config/kafka";
import { TOPICS } from "../topics/topic.names";
import { BaseEvent, PaymentProcessPayload } from "../utils/event.factory";

export async function publishPaymentProcess(
  event: BaseEvent<PaymentProcessPayload>
): Promise<void> {
  const producer = kafka.producer();

  try {
    console.log("Connecting payment producer...");
    await producer.connect();
    console.log("Payment producer connected.");

    const result = await producer.send({
      topic: TOPICS.PAYMENT_PROCESS,
      messages: [
        {
          key: event.key,
          value: JSON.stringify(event),
        },
      ],
    });

    console.log("payment.process event published successfully.");
    console.log("Kafka result:", result);
  } catch (error) {
    console.error("Failed to publish payment.process event:", error);
    throw error;
  } finally {
    await producer.disconnect();
    console.log("Payment producer disconnected.");
  }
}