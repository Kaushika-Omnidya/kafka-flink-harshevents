import mqtt from "mqtt";
import { Kafka } from "kafkajs";

console.log("Bridge starting...");

const MQTT_URL = "mqtt://localhost:1883";
const MQTT_TOPIC = "test/topic";

const kafka = new Kafka({
  clientId: "mqtt-kafka-bridge",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();

(async () => {
  await producer.connect();
  console.log("Connected to Kafka");

  const client = mqtt.connect(MQTT_URL);

  client.on("connect", () => {
    console.log("Connected to MQTT");
    client.subscribe(MQTT_TOPIC, (err) => {
      if (err) console.error("MQTT subscribe error:", err);
      else console.log("Subscribed:", MQTT_TOPIC);
    });
  });

  client.on("message", async (topic, message) => {
    console.log("MQTT:", topic, message.toString());
    try {
      await producer.send({
        topic: "telemetry.raw",
        messages: [{ key: "device1", value: message.toString() }],
      });
      console.log("Sent to Kafka: telemetry.raw");
    } catch (e) {
      console.error("Kafka send error:", e);
    }
  });

  client.on("error", (err) => console.error("MQTT error:", err));
  client.on("reconnect", () => console.log("MQTT reconnecting..."));
  client.on("close", () => console.log("MQTT closed"));
})();
