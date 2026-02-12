/**
 * mqttToKafka.js
 * MQTT -> Kafka bridge (CommonJS, Node 10+ compatible)
 *
 * ENV:
 *  MQTT_URL=mqtt://localhost:1883
 *  MQTT_TOPIC=test/topic
 *  KAFKA_BROKERS=localhost:9092
 *  KAFKA_TOPIC=telemetry.raw
 */

require("dotenv").config();
const mqtt = require("mqtt");
const { Kafka } = require("kafkajs");

console.log("Bridge starting...");

// ---------------- CONFIG ----------------
// MQTT
const MQTT_URL = process.env.MQTT_URL || "mqtt://localhost:1883";
const MQTT_TOPIC = process.env.MQTT_TOPIC || "test/topic";

// Kafka
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092").split(",");
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || "telemetry.raw";

const LOG_EVERY_N = Number(process.env.LOG_EVERY_N || 20);

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
    if (!s || !String(s).trim()) return null;

    let json = String(s).trim();

    // Handle double-encoded JSON string if it ever happens
    if (json.startsWith('"') && json.endsWith('"')) {
      json = json
        .slice(1, -1)
        .replace(/\\"/g, '"')
        .replace(/\\\\/g, "\\");
    }

    return JSON.parse(json);
  } catch {
    return null;
  }
}

function getDeviceUuid(obj) {
  const v = obj && obj.device_uuid;
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
  let warnedMissingTopic = false;

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
        messages: [{ key: deviceUuid, value: JSON.stringify(obj) }],
      });

      count++;

      if (count % LOG_EVERY_N === 0 || Date.now() - lastLog > 15000) {
        lastLog = Date.now();
        console.log(
          `➡️ MQTT->Kafka OK | topic=${KAFKA_TOPIC} | device_uuid=${deviceUuid} | total=${count}`
        );
      }
    } catch (e) {
      const msg = (e && e.message) ? e.message : String(e);

      // Helpful one-time hint
      if (!warnedMissingTopic && /UnknownTopicOrPartition|UNKNOWN_TOPIC_OR_PARTITION/i.test(msg)) {
        warnedMissingTopic = true;
        console.error(
          `❌ Kafka topic missing: ${KAFKA_TOPIC}. Create it inside Kafka container:\n` +
          `sudo docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic ${KAFKA_TOPIC} --partitions 6 --replication-factor 1`
        );
      }

      console.error("❌ Kafka send error:", msg);
    }
  });

  client.on("error", (err) => console.error("❌ MQTT error:", err));
  client.on("reconnect", () => console.log("🔄 MQTT reconnecting..."));
  client.on("close", () => console.log("🔌 MQTT closed"));

  const shutdown = async () => {
    console.log("\nShutting down bridge...");
    try { client.end(true); } catch {}
    try { await producer.disconnect(); } catch {}
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
})().catch((e) => {
  console.error("❌ Bridge fatal error:", e);
  process.exit(1);
});
