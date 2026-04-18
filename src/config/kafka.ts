import { Kafka } from "kafkajs";

export const kafka = new Kafka({
  clientId: "kafka-order-lab",
  brokers: ["localhost:29092"],
})