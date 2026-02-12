import mqtt from "mqtt";
import { Kafka } from "kafkajs";

console.log("Bridge starting...");

// ---------------- CONFIG ----------------
// MQTT
const MQTT_URL = process.env.MQTT_URL || "mqtt://localhost:1883";
const MQTT_TOPIC = process.env.MQTT_TOPIC || "test/topic";

// Kafka
// If this bridge runs on HOST -> localhost:9092
// If this bridge runs INSIDE docker-compose network -> kafka:9094
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092").split(",");
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || "telemetry.raw";

const LOG_EVERY_N = Number(process.env.LOG_EVERY_N || 20); // print 1 in N messages

// ---------------- Kafka Producer ----------------
const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || "mqtt-kafka-bridge",
  brokers: KAFKA_BROKERS,
});

const producer = kafka.producer({
  allowAutoTopicCreation: false,
});

// ---------------- Helpers ----------------
function safeJsonParse(s) {
  try {
    if (!s || !s.trim()) return null;

    // Handle double-encoded JSON string if it ever happens
    let json = s.trim();
    if (json.startsWith('"') && json.endsWith('"')) {
      json = json.substring(1, json.length - 1)
        .replace(/\\"/g, '"')
        .replace(/\\\\/g, "\\");
    }
    return JSON.parse(json);
  } catch {
    return null;
  }
}

function getDeviceUuid(obj) {
  const v = obj?.device_uuid;
  if (typeof v === "string" && v.trim()) return v.trim();
  return "unknown-device";
}

// ---------------- Main ----------------
(async () => {
  await producer.connect();
  console.log("✅ Connected to Kafka:", KAFKA_BROKERS.join(","));

  const client = mqtt.connect(MQTT_URL, {
    reconnectPeriod: 1000,
    connectTimeout: 30000,
    keepalive: 60,
    clean: true,
  });

  let count = 0;
  let lastLog = Date.now();

  client.on("connect", () => {
    console.log("✅ Connected to MQTT:", MQTT_URL);

    client.subscribe(MQTT_TOPIC, { qos: 0 }, (err) => {
      if (err) console.error("❌ MQTT subscribe error:", err);
      else console.log("📌 Subscribed:", MQTT_TOPIC);
    });
  });

  client.on("message", async (topic, message) => {
    const raw = message.toString();

    const obj = safeJsonParse(raw);
    if (!obj) {
      console.error("⚠️ Skipping invalid JSON:", raw.slice(0, 200));
      return;
    }

    const deviceUuid = getDeviceUuid(obj);

    try {
      await producer.send({
        topic: KAFKA_TOPIC,
        messages: [
          {
            key: deviceUuid,    // ✅ IMPORTANT: key by device_uuid for Flink keyBy
            value: JSON.stringify(obj),
          },
        ],
      });

      count++;

      // Throttled logs
      if (count % LOG_EVERY_N === 0 || Date.now() - lastLog > 15000) {
        lastLog = Date.now();
        console.log(
          `➡️ MQTT->Kafka OK | topic=${KAFKA_TOPIC} | device_uuid=${deviceUuid} | total=${count}`
        );
      }
    } catch (e) {
      console.error("❌ Kafka send error:", e?.message || e);
    }
  });

  client.on("error", (err) => console.error("❌ MQTT error:", err));
  client.on("reconnect", () => console.log("🔄 MQTT reconnecting..."));
  client.on("close", () => console.log("🔌 MQTT closed"));

  // Graceful shutdown
  const shutdown = async () => {
    console.log("\nShutting down bridge...");
    try {
      client.end(true);
    } catch {}
    try {
      await producer.disconnect();
    } catch {}
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
})();
