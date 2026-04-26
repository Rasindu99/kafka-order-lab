import { kafka } from "../config/kafka";
import { TOPICS } from "../topics/topic.names";
import { 
  BaseEvent, 
  PaymentProcessPayload, 
  createPaymentCompletedEvent, 
  createPaymentFailedEvent 
} from "../utils/event.factory";
import { publishPaymentCompleted } from "../producers/payment.completed.producer";
import { publishPaymentFailed } from "../producers/payment.failed.producer";

function shouldPaymentSucceed(): boolean {
  return Math.random() < 0.7; // 70% chance of success
}

function createPaymentId(orderId: string): string {
  return `pay-${orderId}-${Date.now()}`;
}

async function runConsumer() {
  const consumer = kafka.consumer({
    groupId: "payment-process-consumer-group",
  });

  try {
    console.log("Connecting payment.process consumer...");
    await consumer.connect();
    console.log("Payment.process consumer connected.");

    await consumer.subscribe({
      topic: TOPICS.PAYMENT_PROCESS,
      fromBeginning: true,
    });

    console.log(`Subscribed to topic "${TOPICS.PAYMENT_PROCESS}".`);
    console.log("Waiting for payment.process messages...\n");

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const rawValue = message.value?.toString() ?? "";

        if (!rawValue) {
          console.log("Skipping empty message.");
          return;
        }
        
        const parsedValue = JSON.parse(
          rawValue
        ) as BaseEvent<PaymentProcessPayload>;

        console.log("Received payment.process message:");
        console.log({
          topic,
          partition,
          offset: message.offset,
          key: message.key?.toString(),
          value: parsedValue,
          timestamp: message.timestamp,
        });

        const success = shouldPaymentSucceed();

        if(success) {
          const completedEvent = createPaymentCompletedEvent({
            orderId: parsedValue.payload.orderId,
            customerId: parsedValue.payload.customerId,
            amount: parsedValue.payload.amount,
            paymentId: createPaymentId(parsedValue.payload.orderId),
            paymentStatus: "COMPLETED",
          });

          await publishPaymentCompleted(completedEvent);
          console.log("Produced payment.completed event:");
          console.log(completedEvent);
        } else {
          const failedEvent = createPaymentFailedEvent({
            orderId: parsedValue.payload.orderId,
            customerId: parsedValue.payload.customerId,
            amount: parsedValue.payload.amount,
            reason: "Simulated payment gateway failure",
            paymentStatus: "FAILED",
          });
          
          await publishPaymentFailed(failedEvent);
          console.log("Produced payment.failed event:");
          console.log(failedEvent);
        }

        console.log("--------------------------------------------------");
      },
    });
  } catch (error) {
    console.error("payment.process consumer failed:", error);
    await consumer.disconnect();
  }
}

runConsumer();