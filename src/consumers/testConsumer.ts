import { kafka } from "../config/kafka";

async function runConsumer() {
  const consumer = kafka.consumer({
    groupId: "test-consumer-group",
  });

  try {
    console.log("Connecting consumer...");
    await consumer.connect();
    console.log("Consumer connected.");

  /* 
    Within the same consumer group, one partition is handled by only one consumer at a time. 

    So if your topic has 3 partitions:
     * 1 consumer in the group → it may read all 3 partitions
     * 2 consumers in the same group → they split the partitions
     * 3 consumers in the same group → each may get one partition
     * 4 consumers in the same group → one stays idle because only 3 partitions exist
    This is a core Kafka scaling model.
  */

    await consumer.subscribe({
      topic: "test.logs",
      fromBeginning: true,
    });

    // if the topic already contains old messages, this consumer can read them too.
    // This is very useful because you want to see previously produced test messages.
    // If you set fromBeginning: false, then the consumer would usually start from the latest position and only see new incoming messages.

    // fromBeginning: true does not always mean it will reread everything forever.
    // If test-consumer-group has already read messages and committed offsets, Kafka will usually resume from the committed offset, not from zero again.

    console.log('Subscribed to topic "test.logs".');
    console.log("Waiting for messages...\n");

    // Once run() starts, the consumer keeps listening for messages until the app stops or crashes.
    // Each incoming message triggers the eachMessage callback, where you can process the message as needed.
    // They are not variables you created earlier.
    // They are parameters supplied by the KafkaJS library when a message arrives.
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const rawValue = message.value?.toString() ?? "";
        const parsedValue = rawValue ? JSON.parse(rawValue) : null;

        console.log("Received message:");
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
    console.error("Consumer failed:", error);
    await consumer.disconnect();
  }
}

runConsumer();