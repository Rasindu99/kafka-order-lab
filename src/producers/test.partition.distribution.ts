import { kafka } from "../config/kafka";
import { TOPICS } from "../topics/topic.names";

async function testPartitionDistribution() {
  const producer = kafka.producer();

  try {
    console.log("Connecting producer...");
    await producer.connect();
    console.log("Producer connected.\n");

    for (let i = 1; i <= 30; i++) {
      const customerId = `customer-${i}`;

      const event = {
        eventId: `evt-${Date.now()}-${i}`,
        eventType: "order.created",
        eventVersion: 1,
        occurredAt: new Date().toISOString(),
        source: "partition-test",
        key: customerId,
        payload: {
          orderId: `order-${i}`,
          customerId,
          amount: i * 10,
          items: [
            {
              sku: `sku-${i}`,
              quantity: 1,
            },
          ],
        },
      };

      const result = await producer.send({
        topic: TOPICS.ORDER_CREATED,
        messages: [
          {
            key: customerId,
            value: JSON.stringify(event),
          },
        ],
      });

      console.log(
        `Sent key=${customerId} -> partition=${result[0].partition}, offset=${result[0].baseOffset}`
      );
    }
  } catch (error) {
    console.error("Partition test failed:", error);
  } finally {
    await producer.disconnect();
    console.log("\nProducer disconnected.");
  }
}

testPartitionDistribution();