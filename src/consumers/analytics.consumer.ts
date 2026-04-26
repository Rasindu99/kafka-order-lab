import { kafka } from "../config/kafka";
import { TOPICS } from "../topics/topic.names";
import { BaseEvent, PaymentCompletedPayload } from "../utils/event.factory";

let totalCompletedPayments = 0;
let totalRevenue = 0;

async function runConsumer() {
  const consumer = kafka.consumer({
    groupId: "analytics-consumer-group",
  });

  try {
    console.log("Connecting analytics consumer...");
    await consumer.connect();
    console.log("Analytics consumer connected.");

    await consumer.subscribe({
      topic: TOPICS.PAYMENT_COMPLETED,
      fromBeginning: true,
    });

    console.log(`Subscribed to topic "${TOPICS.PAYMENT_COMPLETED}".`);
    console.log("Waiting for payment.completed messages in analytics...\n");

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const rawValue = message.value?.toString() ?? "";

        if (!rawValue) {
          console.log("Skipping empty analytics message.");
          return;
        }

        const parsedValue = JSON.parse(
          rawValue
        ) as BaseEvent<PaymentCompletedPayload>;

        totalCompletedPayments += 1;
        totalRevenue += parsedValue.payload.amount;

        console.log("Analytics received payment.completed:");
        console.log({
          topic,
          partition,
          offset: message.offset,
          key: message.key?.toString(),
          orderId: parsedValue.payload.orderId,
          customerId: parsedValue.payload.customerId,
          amount: parsedValue.payload.amount,
          totalCompletedPayments,
          totalRevenue,
        });
        console.log("--------------------------------------------------");
      },
    });
  } catch (error) {
    console.error("analytics consumer failed:", error);
    await consumer.disconnect();
  }
}

runConsumer();