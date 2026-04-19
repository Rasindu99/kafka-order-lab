import { kafka } from "../config/kafka";
// This imports your shared Kafka client config.

async function runProducer() {
  const producer = kafka.producer();

  try {
    console.log("Connecting producer...");
    await producer.connect();
    console.log("Producer connected.");

    const event = {
      eventId: `evt-${Date.now()}`,
      eventType: "test.log.created",
      occurredAt: new Date().toISOString(),
      source: "test-producer",
      payload: {
        message: "Hello Kafka from Node.js",
      },
    };

    // The key helps Kafka decide which partition the message should go to.
    // Using a consistent key ensures all messages for the same user go to the same partition.
    const key = "user-123";

    // Topic tells Kafka where the message should go.
    const result = await producer.send({
      topic: "test.logs",
      messages: [
        {
          key,
          value: JSON.stringify(event),
        },
      ],
    });
    
    // Kafka messages are sent as bytes/string-like data, not raw JavaScript objects.
    // So before sending, your object is converted into a JSON string
    // Usually send strings or buffers as message values, and Kafka will handle them as bytes.

    console.log("Message sent successfully.");
    console.log("Kafka result:", result);
    /*
    result usually contains metadata like:
      * topic name
      * partition used
      * offset of the message ( position inside that partition )
      * timestamp of the message
    */
    console.log("Sent event:", event);
  } catch (error) {
    console.error("Producer failed:", error);
  } finally {
    await producer.disconnect();
    console.log("Producer disconnected.");
  }
}

runProducer();