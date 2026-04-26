import { kafka } from "../config/kafka";
import { TOPICS } from "../topics/topic.names";

async function createTopics() {
  const admin = kafka.admin();

  try {
    console.log("Connecting admin client...");
    await admin.connect();
    console.log("Admin connected.");

    // This returns true if the topic was created, false if it already exists
    // In case even one topic already exists, this will return false.
    const created = await admin.createTopics({
      waitForLeaders: true,
      topics: [
        {
          topic: TOPICS.TEST_LOGS,
          numPartitions: 3,
          replicationFactor: 1,
        },
        {
          topic: TOPICS.ORDER_CREATED,
          numPartitions: 3,
          replicationFactor: 1,
        },
        {
          topic: TOPICS.PAYMENT_PROCESS,
          numPartitions: 3,
          replicationFactor: 1,
        },
        {
          topic: TOPICS.PAYMENT_COMPLETED,
          numPartitions: 3,
          replicationFactor: 1
        },
        {
          topic: TOPICS.PAYMENT_FAILED,
          numPartitions: 3,
          replicationFactor: 1
        }
      ],
    });

    if (created) {
      console.log(`Topics created successfully.`);
    } else {
      console.log('Topics already exist.');
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

When you rerun the file after adding a new topic:

  * existing topics are generally unaffected
  * their messages stay
  * their offsets stay
  * only missing topics get added
  * the returned boolean is not a per-topic report and may be false even when new topics were created
  * if return is true, all topics were created successfully
  * if return is false, at least one topic already existed, but new topics may have been created
  
'*/