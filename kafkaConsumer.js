/**
 * kafkaConsumer.js
 *
 * Consumes telemetry.processed -> writes latest state to MongoDB
 * Uses Redis cache to store device_uuid -> Mongo _id for fast updates
 * Tracks end-to-end latency using mqtt_sent_at_ms
 *
 * Works with payload that contains:
 *  - location: { type:"Point", coordinates:[lon,lat] }
 *  - violations: [{ timestamp, type, accel_y, speed_kph, delta_speed }]
 */

const { Kafka } = require("kafkajs");
const mongoose = require("mongoose");
const redis = require("redis");
const fs = require("fs").promises;
const path = require("path");

// ---------------- CONFIG ----------------
// If consumer runs on host: localhost:9092
// If runs inside docker network: kafka:9094
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092").split(",");
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || "telemetry.processed";
const KAFKA_GROUP_ID = process.env.KAFKA_GROUP_ID || "flink-to-db";

const MONGO_URI = process.env.MONGODB_URI || "mongodb://localhost:27017/das";
const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";

const COLLECTION_NAME = process.env.MONGO_COLLECTION || "live-location-telemetry";

// Redis key prefix
const DEVICE_DOC_PREFIX = "device:doc:";

// Latency file
const LATENCY_FILE = path.join(__dirname, "latencies.json");
const latencyBuffer = [];
const LATENCY_FLUSH_INTERVAL = 5000;

// ---------------- Kafka ----------------
const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || "db-writer",
  brokers: KAFKA_BROKERS,
});

const consumer = kafka.consumer({ groupId: KAFKA_GROUP_ID });

// ---------------- MongoDB ----------------
mongoose
  .connect(MONGO_URI)
  .then(() => console.log("✅ MongoDB connected:", MONGO_URI))
  .catch((err) => console.error("❌ MongoDB connection error:", err));

// ---------------- Redis ----------------
const redisClient = redis.createClient({ url: REDIS_URL });
redisClient.on("error", (err) => console.error("❌ Redis error:", err));
redisClient.on("connect", () => console.log("✅ Redis connected:", REDIS_URL));

(async () => {
  await redisClient.connect();
})().catch((e) => console.error("❌ Redis connect failed:", e));

// ---------------- Schema ----------------
const violationSchema = new mongoose.Schema(
  {
    timestamp: Number,
    type: String,         // "harsh_brake" | "harsh_accel" etc.
    accel_y: Number,
    speed_kph: Number,
    delta_speed: Number,
  },
  { _id: false }
);

const telemetrySchema = new mongoose.Schema(
  {
    timestamp: { type: Number, required: true, index: true },
    device_uuid: { type: String, required: true, index: true },
    vehicle_id: { type: String, required: true, index: true },
    account_id: { type: String, required: true, index: true },

    // Location (your payload contains only "location")
    location: {
      type: { type: String, enum: ["Point"], default: "Point" },
      coordinates: { type: [Number], required: true }, // [lon, lat]
    },

    lat_dir: String,
    lon_dir: String,

    // Your payload uses 1/0, not true/false
    location_changed: { type: Number, default: 0 },

    speed_kph: Number,
    speed_mph: Number,

    ontrip: Boolean,

    // Updated: array of objects (not strings)
    violations: { type: [violationSchema], default: [] },

    // Device and sensor data
    mqtt_sent_at_ms: Number,
    fix_quality: String,
    temp_C: Number,
    accel_x: Number,
    accel_y: Number,
    accel_z: Number,
    gyro_x: Number,
    gyro_y: Number,
    gyro_z: Number,
    cpu_temp: Number,
    soc_temp: Number,
    main_board_temp: Number,

    // Network data
    sim_iccid: String,
    sim_imsi: String,
    signal_strength_percent: Number,

    // Device status
    imu_is_stopped: Boolean,
    dashcam_power_source: String,
    battery_capacity: Number,
  },
  {
    timestamps: true,
    collection: COLLECTION_NAME,
  }
);

// Indexes
telemetrySchema.index({ location: "2dsphere" });
telemetrySchema.index({ vehicle_id: 1, timestamp: -1 });
telemetrySchema.index({ device_uuid: 1, timestamp: -1 });
telemetrySchema.index({ account_id: 1, timestamp: -1 });

const Telemetry = mongoose.model("Telemetry", telemetrySchema);

// ---------------- Latency file helpers ----------------
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
  } catch (error) {
    console.error("❌ Error writing latencies:", error);
  }
}

setInterval(flushLatencies, LATENCY_FLUSH_INTERVAL);

// ---------------- JSON parsing (robust) ----------------
function safeParseJSON(str) {
  try {
    if (!str || !str.trim()) return null;

    let s = str.trim();

    // Handle double-encoded json like "\"{...}\""
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

// ---------------- Consumer logic ----------------
const run = async () => {
  await consumer.connect();
  console.log("✅ Kafka consumer connected:", KAFKA_BROKERS.join(","));

  await initLatencyFile();

  await consumer.subscribe({
    topic: KAFKA_TOPIC,
    fromBeginning: false,
  });

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const receivedAt = Date.now();
        const raw = message.value.toString();

        const telemetryData = safeParseJSON(raw);
        if (!telemetryData) {
          console.error("⚠️ Skipping invalid JSON:", raw.slice(0, 200));
          return;
        }

        const deviceUuid = telemetryData.device_uuid;
        if (!deviceUuid) {
          console.error("⚠️ Missing device_uuid, skipping record");
          return;
        }

        // End-to-end latency (if mqtt_sent_at_ms exists)
        if (telemetryData.mqtt_sent_at_ms) {
          const latency = receivedAt - telemetryData.mqtt_sent_at_ms;

          latencyBuffer.push({
            device_uuid: deviceUuid,
            mqtt_sent_at_ms: telemetryData.mqtt_sent_at_ms,
            kafka_received_at_ms: receivedAt,
            latency_ms: latency,
            timestamp: new Date(receivedAt).toISOString(),
          });

          console.log(
            `📥 ${deviceUuid} ts=${new Date(telemetryData.timestamp * 1000).toISOString()} latency=${latency}ms`
          );
        } else {
          console.log(
            `📥 ${deviceUuid} ts=${new Date(telemetryData.timestamp * 1000).toISOString()}`
          );
        }

        // Redis cache lookup
        const cacheKey = `${DEVICE_DOC_PREFIX}${deviceUuid}`;
        let docId = await redisClient.get(cacheKey);

        // Ensure required fields exist for schema
        // If location is missing, skip because geo index requires it
        if (!telemetryData.location || !telemetryData.location.coordinates) {
          console.error(`⚠️ Missing location for ${deviceUuid}, skipping`);
          return;
        }

        if (docId) {
          // Fast update by _id
          await Telemetry.findByIdAndUpdate(docId, telemetryData, { new: false });
          console.log(
            `✓ [CACHED] Updated ${deviceUuid} | speed_kph=${telemetryData.speed_kph} | violations=${(telemetryData.violations || []).length}`
          );
        } else {
          // Upsert by device_uuid, then cache the _id
          const doc = await Telemetry.findOneAndUpdate(
            { device_uuid: deviceUuid },
            telemetryData,
            { upsert: true, new: true, setDefaultsOnInsert: true }
          );

          await redisClient.set(cacheKey, doc._id.toString());
          console.log(
            `✓ [NEW] Cached ${deviceUuid} -> ${doc._id} | speed_kph=${telemetryData.speed_kph} | violations=${(telemetryData.violations || []).length}`
          );
        }
      } catch (error) {
        console.error("❌ Error processing message:", error);
      }
    },
  });
};

// Restart logic
consumer.on("consumer.crash", async (event) => {
  console.error("❌ Consumer crashed:", event.payload.error);
  try {
    await run();
  } catch (e) {
    console.error("❌ Failed to restart consumer:", e);
  }
});

consumer.on("consumer.disconnect", () => {
  console.log("⚠️ Consumer disconnected");
});

// Graceful shutdown
const shutdown = async () => {
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
    await redisClient.quit();
  } catch {}

  console.log("✅ All connections closed");
  process.exit(0);
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

run().catch(console.error);
