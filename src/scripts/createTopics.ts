import { kafka } from "../config/kafka";

async function createTopics() {
  const admin = kafka.admin();

  try {
    console.log("Connecting admin client...");
    await admin.connect();
    console.log("Admin connected.");

    // This returns true if the topic was created, false if it already exists
    const created = await admin.createTopics({
      waitForLeaders: true,
      topics: [
        {
          topic: "test.logs",
          numPartitions: 3,
          replicationFactor: 1,
        },
      ],
    });

    if (created) {
      console.log('Topic "test.logs" created successfully.');
    } else {
      console.log('Topic "test.logs" already exists.');
    }

    const topics = await admin.listTopics();
    console.log("Available topics:", topics);
  } catch (error) {
    console.error("Failed to create topics:", error);
  } finally {
    await admin.disconnect();
    console.log("Admin disconnected.");
  }
}

createTopics();

/*

This is your first use of the Admin API.

It does three things:

  * connects to Kafka
  * creates a topic called test.logs
  * lists topics to verify it's creation

'*/