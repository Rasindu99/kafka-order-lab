import { Kafka } from "kafkajs";

// create a shared Kafka client instance that can be imported across your app
export const kafka = new Kafka({
  clientId: "kafka-order-lab",
  brokers: ["localhost:29092"],
})