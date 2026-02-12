/**
 * kafkaConsumer.js (Option A)
 *
 * Consumes:
 *   - violations.events
 *   - device-status.events
 *
 * Writes:
 *   - MongoDB collection: dashcam_captured_event (default)
 *
 * Implements:
 *   - Violation event inserts (1 doc per violation event)
 *   - Cable-unplugged session consolidation (touch/clear) using Redis TTL
 *   - Redis counters: metrics:violations:<type>
 *   - Latency tracking to latencies.json using mqtt_sent_at_ms if present
 *
 * NOTE: This code expects Node.js 18+ (recommended). Your current Node 10 will break with modern deps.
 */

const { Kafka, logLevel } = require("kafkajs");
const mongoose = require("mongoose");
const redis = require("redis");
const fs = require("fs").promises;
const path = require("path");

// ---------------- CONFIG ----------------
// Kafka
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092").split(",");
const KAFKA_GROUP_ID = process.env.KAFKA_GROUP_ID || "events-to-mongo";
const TOPIC_VIOLATIONS = process.env.TOPIC_VIOLATIONS || "violations.events";
const TOPIC_STATUS = process.env.TOPIC_STATUS || "device-status.events";

// Mongo
const MONGO_URI = process.env.MONGODB_URI || "mongodb://localhost:27017/das";
const MONGO_DB = process.env.MONGODB_DATABASE || ""; // optional
const MONGO_COLLECTION = process.env.MONGO_COLLECTION || "dashcam_captured_event";

// Redis
const REDIS_ENABLED = (process.env.REDIS_ENABLED || "true").toLowerCase() === "true";
const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";

// Cable-unplugged session TTL seconds (same as python: 300)
const STATUS_TTL_SECONDS = Number(process.env.STATUS_TTL_SECONDS || 300);

// Redis keys
const ACTIVE_STATUS_KEY_PREFIX = "active_status:cable-unplugged:"; // + device_uuid
const METRICS_KEY_PREFIX = "metrics:violations:"; // + violation_type

// Latency
const LATENCY_FILE = process.env.LATENCY_FILE || path.join(__dirname, "latencies.json");
const LATENCY_FLUSH_INTERVAL = Number(process.env.LATENCY_FLUSH_INTERVAL || 5000);
const latencyBuffer = [];

// ---------------- Helpers ----------------
function safeParseJSON(str) {
  try {
    if (!str || !str.trim()) return null;
    let s = str.trim();

    // handle double-encoded JSON like "\"{...}\""
    if (s.startsWith('"') && s.endsWith('"')) {
      s = s
        .slice(1, -1)
        .replace(/\\"/g, '"')
        .replace(/\\\\/g, "\\");
    }

    return JSON.parse(s);
  } catch {
    return null;
  }
}

async function initLatencyFile() {
  try {
    await fs.access(LATENCY_FILE);
    console.log("📊 Latency file exists, will append:", LATENCY_FILE);
  } catch {
    await fs.writeFile(LATENCY_FILE, "[]", "utf8");
    console.log("📊 Created latency file:", LATENCY_FILE);
  }
}

async function flushLatencies() {
  if (latencyBuffer.length === 0) return;

  try {
    const fileContent = await fs.readFile(LATENCY_FILE, "utf8");
    const existing = JSON.parse(fileContent);
    existing.push(...latencyBuffer);

    await fs.writeFile(LATENCY_FILE, JSON.stringify(existing, null, 2), "utf8");
    console.log(`📊 Flushed ${latencyBuffer.length} latency records (Total: ${existing.length})`);
    latencyBuffer.length = 0;
  } catch (err) {
    console.error("❌ Error writing latencies:", err);
  }
}

setInterval(() => {
  flushLatencies().catch(() => {});
}, LATENCY_FLUSH_INTERVAL);

// ---------------- Mongo Schema ----------------
/**
 * We store both:
 *  - violation docs
 *  - device_status session docs
 *
 * Similar to your Python "dashcam_captured_event" concept.
 */
const eventSchema = new mongoose.Schema(
  {
    // common
    event_type: { type: String, required: true, index: true }, // "violation" | "device_status"
    device_uuid: { type: String, required: true, index: true },
    vehicle_id: { type: String, index: true },
    account_id: { type: String, index: true },

    timestamp: { type: Number, required: true, index: true }, // event timestamp (seconds)
    mqtt_sent_at_ms: { type: Number }, // optional

    // geo (optional but recommended)
    location: {
      type: { type: String, enum: ["Point"], default: "Point" },
      coordinates: { type: [Number] }, // [lon, lat]
    },

    // violation-specific
    violation_type: { type: String, index: true },
    details: {
      accel_y: Number,
      speed_kph: Number,
      delta_speed: Number,
    },

    // device status (session) specific
    status_type: { type: String, index: true }, // "cable-unplugged"
    start_timestamp: { type: Number }, // seconds
    end_timestamp: { type: Number },   // seconds
  },
  {
    timestamps: true,
    collection: MONGO_COLLECTION,
  }
);

eventSchema.index({ location: "2dsphere" });
eventSchema.index({ device_uuid: 1, timestamp: -1 });
eventSchema.index({ event_type: 1, timestamp: -1 });
eventSchema.index({ violation_type: 1, timestamp: -1 });
eventSchema.index({ status_type: 1, timestamp: -1 });

const CapturedEvent = mongoose.model("CapturedEvent", eventSchema);

// ---------------- Connections ----------------
async function connectMongo() {
  await mongoose.connect(MONGO_URI);
  console.log("✅ MongoDB connected:", MONGO_URI);
  if (MONGO_DB) {
    console.log("ℹ️  MONGODB_DATABASE is set but mongoose uses db from URI by default:", MONGO_DB);
  }
}

let redisClient = null;

async function connectRedis() {
  if (!REDIS_ENABLED) {
    console.log("⚠️ Redis disabled (REDIS_ENABLED=false). Status session consolidation will be limited.");
    return;
  }
  redisClient = redis.createClient({ url: REDIS_URL });
  redisClient.on("error", (err) => console.error("❌ Redis error:", err));
  redisClient.on("connect", () => console.log("✅ Redis connected:", REDIS_URL));
  await redisClient.connect();
}

// ---------------- Processing: Violations ----------------
async function handleViolationEvent(ev, receivedAtMs) {
  // expected:
  // {
  //   event_type:"violation",
  //   violation_type:"harsh_brake",
  //   device_uuid:"...",
  //   timestamp:1769...,
  //   location:{type:"Point",coordinates:[lon,lat]},
  //   details:{...},
  //   mqtt_sent_at_ms:...
  // }

  const deviceUuid = ev.device_uuid;
  const violationType = ev.violation_type;
  const ts = ev.timestamp;

  if (!deviceUuid || !violationType || !ts) {
    console.error("⚠️ Invalid violation event, missing device_uuid/violation_type/timestamp:", ev);
    return;
  }

  // latency tracking (optional)
  if (ev.mqtt_sent_at_ms) {
    const latency = receivedAtMs - ev.mqtt_sent_at_ms;
    latencyBuffer.push({
      topic: TOPIC_VIOLATIONS,
      device_uuid: deviceUuid,
      mqtt_sent_at_ms: ev.mqtt_sent_at_ms,
      kafka_received_at_ms: receivedAtMs,
      latency_ms: latency,
      timestamp: new Date(receivedAtMs).toISOString(),
    });
  }

  // Insert violation document (1 per event)
  const doc = {
    event_type: "violation",
    device_uuid: deviceUuid,
    vehicle_id: ev.vehicle_id,
    account_id: ev.account_id,
    timestamp: ts,
    mqtt_sent_at_ms: ev.mqtt_sent_at_ms,
    location: ev.location,
    violation_type: violationType,
    details: ev.details || {},
  };

  const saved = await CapturedEvent.create(doc);

  // Redis metric counter
  if (redisClient) {
    const key = `${METRICS_KEY_PREFIX}${violationType}`;
    await redisClient.incr(key);
  }

  console.log(`✅ [VIOLATION] stored type=${violationType} device=${deviceUuid} _id=${saved._id}`);
}

// ---------------- Processing: Device Status (cable-unplugged sessions) ----------------
async function handleDeviceStatusEvent(ev, receivedAtMs) {
  // expected:
  // {
  //   event_type:"device_status",
  //   status_type:"cable-unplugged",
  //   action:"touch"|"clear",
  //   device_uuid:"...",
  //   timestamp:...,
  //   location:{...}
  // }

  const deviceUuid = ev.device_uuid;
  const statusType = ev.status_type;
  const action = ev.action;
  const ts = ev.timestamp;

  if (!deviceUuid || !statusType || !action || !ts) {
    console.error("⚠️ Invalid device-status event:", ev);
    return;
  }

  // latency tracking (optional)
  if (ev.mqtt_sent_at_ms) {
    const latency = receivedAtMs - ev.mqtt_sent_at_ms;
    latencyBuffer.push({
      topic: TOPIC_STATUS,
      device_uuid: deviceUuid,
      mqtt_sent_at_ms: ev.mqtt_sent_at_ms,
      kafka_received_at_ms: receivedAtMs,
      latency_ms: latency,
      timestamp: new Date(receivedAtMs).toISOString(),
    });
  }

  // We only consolidate "cable-unplugged"
  if (statusType !== "cable-unplugged") {
    console.log(`ℹ️ [STATUS] Ignoring unsupported status_type=${statusType}`);
    return;
  }

  const redisKey = `${ACTIVE_STATUS_KEY_PREFIX}${deviceUuid}`;

  if (action === "touch") {
    if (!redisClient) {
      // Without redis, just create a new session each time (not ideal)
      const created = await CapturedEvent.create({
        event_type: "device_status",
        status_type: "cable-unplugged",
        device_uuid: deviceUuid,
        vehicle_id: ev.vehicle_id,
        account_id: ev.account_id,
        timestamp: ts,
        start_timestamp: ts,
        end_timestamp: ts,
        location: ev.location,
        mqtt_sent_at_ms: ev.mqtt_sent_at_ms,
      });
      console.log(`✅ [STATUS touch/no-redis] created new session _id=${created._id} device=${deviceUuid}`);
      return;
    }

    // With redis: update existing session if present
    const activeEventId = await redisClient.get(redisKey);

    if (activeEventId) {
      // Update end_timestamp
      const updated = await CapturedEvent.findByIdAndUpdate(
        activeEventId,
        { $set: { end_timestamp: ts, timestamp: ts } }, // keep timestamp as "last touch"
        { new: true }
      );

      if (updated) {
        // Refresh TTL
        await redisClient.expire(redisKey, STATUS_TTL_SECONDS);
        console.log(`✅ [STATUS touch] updated session _id=${activeEventId} end_ts=${ts} device=${deviceUuid}`);
        return;
      } else {
        // Mongo doc missing => clear redis and create new
        await redisClient.del(redisKey);
      }
    }

    // Create new session
    const created = await CapturedEvent.create({
      event_type: "device_status",
      status_type: "cable-unplugged",
      device_uuid: deviceUuid,
      vehicle_id: ev.vehicle_id,
      account_id: ev.account_id,
      timestamp: ts,
      start_timestamp: ts,
      end_timestamp: ts,
      location: ev.location,
      mqtt_sent_at_ms: ev.mqtt_sent_at_ms,
    });

    await redisClient.set(redisKey, created._id.toString(), { EX: STATUS_TTL_SECONDS });
    console.log(`✅ [STATUS touch] created new session _id=${created._id} device=${deviceUuid}`);
    return;
  }

  if (action === "clear") {
    // Clear active session key (python: when not battery)
    if (redisClient) {
      const existed = await redisClient.del(redisKey);
      if (existed) console.log(`🧹 [STATUS clear] cleared active session key for device=${deviceUuid}`);
    }
    return;
  }

  console.log(`ℹ️ [STATUS] Unknown action=${action} (ignored)`);
}

// ---------------- Kafka consumer ----------------
const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || "event-writer",
  brokers: KAFKA_BROKERS,
  logLevel: logLevel.NOTHING,
});

const consumer = kafka.consumer({ groupId: KAFKA_GROUP_ID });

// ---------------- Run ----------------
async function run() {
  await connectMongo();
  await connectRedis();
  await initLatencyFile();

  await consumer.connect();
  console.log("✅ Kafka consumer connected:", KAFKA_BROKERS.join(","));

  // Subscribe to both topics
  await consumer.subscribe({ topic: TOPIC_VIOLATIONS, fromBeginning: false });
  await consumer.subscribe({ topic: TOPIC_STATUS, fromBeginning: false });

  console.log(`📥 Subscribed topics: ${TOPIC_VIOLATIONS}, ${TOPIC_STATUS}`);

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      const receivedAtMs = Date.now();
      const raw = message.value ? message.value.toString() : "";
      const ev = safeParseJSON(raw);

      if (!ev) {
        console.error("⚠️ Skipping invalid JSON:", raw.slice(0, 200));
        return;
      }

      try {
        if (topic === TOPIC_VIOLATIONS) {
          await handleViolationEvent(ev, receivedAtMs);
        } else if (topic === TOPIC_STATUS) {
          await handleDeviceStatusEvent(ev, receivedAtMs);
        } else {
          console.log("ℹ️ Message from unknown topic ignored:", topic);
        }
      } catch (err) {
        console.error("❌ Error processing message:", err);
      }
    },
  });
}

// Restart logic
consumer.on("consumer.crash", async (event) => {
  console.error("❌ Consumer crashed:", event.payload.error);
  try {
    await run();
  } catch (e) {
    console.error("❌ Failed to restart consumer:", e);
  }
});

// Graceful shutdown
async function shutdown() {
  console.log("\nShutting down gracefully...");

  try {
    await flushLatencies();
  } catch {}

  try {
    await consumer.disconnect();
  } catch {}

  try {
    await mongoose.connection.close();
  } catch {}

  try {
    if (redisClient) await redisClient.quit();
  } catch {}

  console.log("✅ All connections closed");
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

run().catch((e) => {
  console.error("❌ Fatal error:", e);
  process.exit(1);
});
