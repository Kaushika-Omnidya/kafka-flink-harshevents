/**
 * mqtt_publish.js
 *
 * MQTT load-test publisher for EMQX (local) or AWS IoT Core (optional).
 * Publishes telemetry messages that match harsh-accel / harsh-brake schema.
 *
 * IMPORTANT:
 * - To generate device-status.events you MUST sometimes publish:
 *     dashcam_power_source: "battery"
 */

require("dotenv").config();
const mqtt = require("mqtt");
const fs = require("fs");
const path = require("path");

// -------------------- CONFIG --------------------
const LOCAL = true;
const NUM_DEVICES = 1;
const syncTesting = true;
const TIME_TO_SEND = 1000;

let MQTT_TOPIC = LOCAL ? "test/topic" : "telematics/+";

// If true, device_uuid changes every message (not recommended for Flink state)
const RANDOM_DEVICE_UUID_EACH_MESSAGE = false;

// Violations probability
const VIOLATION_PROB = 0.65;
const FORCE_VIOLATION_ALWAYS = false;

// Probability for unplug event
const BATTERY_PROB = 0.15; // 15% chance battery (unplugged)

// -------------------- Connection --------------------
let connectionConfig;

if (LOCAL) {
  connectionConfig = {
    host: "localhost",
    port: 1883,
    protocol: "mqtt",
    clientId: `loadtest_publisher_${Math.random().toString(16).substring(2, 10)}`,
    keepalive: 60,
    reconnectPeriod: 1000,
    connectTimeout: 30000,
    clean: true,
  };
} else {
  const AWS_IOT_ENDPOINT = process.env.AWS_IOT_ENDPOINT;
  const AWS_CLIENT_ID = process.env.AWS_CLIENT_ID;
  const AWS_CERT_PATH = process.env.AWS_CERT_PATH;
  const AWS_KEY_PATH = process.env.AWS_KEY_PATH;
  const AWS_CA_PATH = process.env.AWS_CA_PATH;

  if (!AWS_IOT_ENDPOINT || !AWS_CLIENT_ID || !AWS_CERT_PATH || !AWS_KEY_PATH || !AWS_CA_PATH) {
    console.error("❌ Missing AWS IoT configuration in .env file");
    process.exit(1);
  }

  connectionConfig = {
    host: AWS_IOT_ENDPOINT,
    port: 8883,
    protocol: "mqtts",
    clientId: `${AWS_CLIENT_ID}_loadtest_${Math.random().toString(16).substring(2, 10)}`,
    cert: fs.readFileSync(path.join(__dirname, AWS_CERT_PATH)),
    key: fs.readFileSync(path.join(__dirname, AWS_KEY_PATH)),
    ca: fs.readFileSync(path.join(__dirname, AWS_CA_PATH)),
    rejectUnauthorized: true,
    keepalive: 60,
    reconnectPeriod: 1000,
    connectTimeout: 30000,
    clean: true,
  };
}

// -------------------- Latency tracking --------------------
const latencyStats = {
  publishLatencies: [],
  addPublishLatency(latency) {
    this.publishLatencies.push(latency);
    if (this.publishLatencies.length > 1000) this.publishLatencies.shift();
  },
  getStats(samples) {
    if (!samples || samples.length === 0) return null;
    const sorted = [...samples].sort((a, b) => a - b);
    const sum = sorted.reduce((a, b) => a + b, 0);
    return {
      count: sorted.length,
      min: sorted[0],
      max: sorted[sorted.length - 1],
      avg: sum / sorted.length,
      p50: sorted[Math.floor(sorted.length * 0.5)],
      p95: sorted[Math.floor(sorted.length * 0.95)],
      p99: sorted[Math.floor(sorted.length * 0.99)],
    };
  },
  printStats() {
    console.log("\n" + "=".repeat(60));
    console.log("📊 LATENCY STATISTICS");
    console.log("=".repeat(60));
    const pubStats = this.getStats(this.publishLatencies);
    if (pubStats) {
      console.log("\n🚀 Publish Latency:");
      console.log(`  Samples: ${pubStats.count}`);
      console.log(`  Min:     ${pubStats.min.toFixed(2)}ms`);
      console.log(`  Avg:     ${pubStats.avg.toFixed(2)}ms`);
      console.log(`  P50:     ${pubStats.p50.toFixed(2)}ms`);
      console.log(`  P95:     ${pubStats.p95.toFixed(2)}ms`);
      console.log(`  P99:     ${pubStats.p99.toFixed(2)}ms`);
      console.log(`  Max:     ${pubStats.max.toFixed(2)}ms`);
    } else {
      console.log("\n⚠️  No latency data collected yet");
    }
    console.log("\n" + "=".repeat(60) + "\n");
  },
};

setInterval(() => latencyStats.printStats(), 30000);

// -------------------- Utils --------------------
function rand(min, max) {
  return Math.random() * (max - min) + min;
}
function randInt(min, max) {
  return Math.floor(rand(min, max + 1));
}
function randBool(pTrue = 0.5) {
  return Math.random() < pTrue;
}
function toFixedNum(x, digits = 6) {
  return Number(x.toFixed(digits));
}
function nowSec() {
  return Math.floor(Date.now() / 1000);
}
function uuidv4() {
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}
function mongoObjectIdLike() {
  const hex = "0123456789abcdef";
  let s = "";
  for (let i = 0; i < 24; i++) s += hex[randInt(0, 15)];
  return s;
}
function generateDevices(n) {
  const out = [];
  for (let i = 1; i <= n; i++) out.push(`device-${i}`);
  return out;
}

// Pools
const ACCOUNT_POOL = [
  "693273ad26df5d305f556728",
  "693273ad26df5d305f556729",
  "693273ad26df5d305f556730",
];
const VEHICLE_POOL = [
  "694ce21db964dc22844b75eb",
  "693ab2683c72f03dfae8ddc5",
  "694ce21db964dc22844b75ec",
];

const devices = generateDevices(NUM_DEVICES);

// -------------------- Telemetry generator --------------------
function generateTelemetryData(deviceUuid) {
  const ts = nowSec();

  const lon = toFixedNum(rand(72.0, 73.5), 6);
  const lat = toFixedNum(rand(21.0, 23.5), 6);

  const speedKph = toFixedNum(rand(0, 90), 1);
  const speedMph = toFixedNum(speedKph * 0.621371, 6);

  // accel_y spike logic
  let accelY = rand(-1.0, 1.0);
  const doSpike = randBool(0.25);
  if (doSpike) accelY = randBool(0.5) ? rand(2.8, 4.5) : rand(-4.5, -2.8);
  accelY = toFixedNum(accelY, 6);

  const accelX = toFixedNum(rand(-1.0, 5.0), 6);
  const accelZ = toFixedNum(9.8 + rand(-1.0, 2.0), 6);

  const includeViolation = FORCE_VIOLATION_ALWAYS ? true : randBool(VIOLATION_PROB);

  let violations = [];
  if (includeViolation) {
    if (accelY <= -2.7) {
      violations = [{
        timestamp: ts,
        type: "harsh_brake",
        accel_y: accelY,
        speed_kph: speedKph,
        delta_speed: toFixedNum(rand(-20, -6), 1),
      }];
    } else if (accelY >= 2.7) {
      violations = [{
        timestamp: ts,
        type: "harsh_accel",
        accel_y: accelY,
        speed_kph: speedKph,
        delta_speed: toFixedNum(rand(6, 20), 1),
      }];
    } else {
      // force one if includeViolation true but accelY small
      if (randBool(0.5)) {
        const spike = toFixedNum(rand(-4.2, -2.8), 6);
        violations = [{
          timestamp: ts,
          type: "harsh_brake",
          accel_y: spike,
          speed_kph: speedKph,
          delta_speed: toFixedNum(rand(-18, -7), 1),
        }];
      } else {
        const spike = toFixedNum(rand(2.8, 4.2), 6);
        violations = [{
          timestamp: ts,
          type: "harsh_accel",
          accel_y: spike,
          speed_kph: speedKph,
          delta_speed: toFixedNum(rand(7, 18), 1),
        }];
      }
    }
  }

  // ✅ Device status generator (battery vs external)
  const powerSource = randBool(BATTERY_PROB) ? "battery" : "external";

  return {
    device_uuid: deviceUuid,
    mqtt_sent_at_ms: Date.now(),
    timestamp: ts,
    fix_quality: "1",
    temp_C: toFixedNum(rand(70, 90), 2),

    accel_x: accelX,
    accel_y: accelY,
    accel_z: accelZ,

    gyro_x: toFixedNum(rand(-0.05, 0.05), 6),
    gyro_y: toFixedNum(rand(-0.05, 0.05), 6),
    gyro_z: toFixedNum(rand(-0.05, 0.05), 6),

    cpu_temp: randInt(60, 80),
    soc_temp: randInt(60, 80),
    main_board_temp: toFixedNum(rand(55, 75), 2),

    sim_iccid: String(randInt(899198200, 899198299)) + String(randInt(1000000000, 9999999999)),
    sim_imsi: "40498" + String(randInt(1000000000, 9999999999)),

    signal_strength_percent: randInt(70, 100),
    imu_is_stopped: randBool(0.1),
    dashcam_power_source: powerSource,   // ✅ IMPORTANT
    battery_capacity: randInt(60, 90),

    lat_dir: "N",
    lon_dir: "E",
    location_changed: 1,

    speed_kph: speedKph,
    speed_mph: speedMph,
    ontrip: true,

    location: {
      type: "Point",
      coordinates: [lon, lat],
    },

    vehicle_id: randBool(0.6)
      ? VEHICLE_POOL[randInt(0, VEHICLE_POOL.length - 1)]
      : mongoObjectIdLike(),
    account_id: randBool(0.7)
      ? ACCOUNT_POOL[randInt(0, ACCOUNT_POOL.length - 1)]
      : mongoObjectIdLike(),

    violations,
  };
}

// -------------------- Publish --------------------
async function publishTelemetry(device) {
  return new Promise((resolve, reject) => {
    const deviceUuid = RANDOM_DEVICE_UUID_EACH_MESSAGE ? uuidv4() : device;
    const telemetry = generateTelemetryData(deviceUuid);

    const publishStartTime = Date.now();

    client.publish(MQTT_TOPIC, JSON.stringify(telemetry), { qos: 0 }, (err) => {
      const publishLatency = Date.now() - publishStartTime;
      if (err) return reject(err);

      latencyStats.addPublishLatency(publishLatency);
      resolve({ device: deviceUuid, publishLatency, power: telemetry.dashcam_power_source });
    });
  });
}

async function sendBurst() {
  const timestamp = new Date().toISOString();
  try {
    const results = await Promise.all(devices.map((d) => publishTelemetry(d)));
    const avg = results.reduce((sum, r) => sum + r.publishLatency, 0) / Math.max(results.length, 1);

    const batteryCount = results.filter((r) => r.power === "battery").length;

    console.log(
      `📤 [${timestamp}] Sent ${NUM_DEVICES} msgs | avgLatency=${avg.toFixed(2)}ms | battery=${batteryCount}/${NUM_DEVICES}`
    );
  } catch (err) {
    console.error("❌ Error during burst:", err);
  }
}

function startLoadTest() {
  if (syncTesting) {
    console.log("🚀 Starting synchronized burst testing...");
    setInterval(sendBurst, TIME_TO_SEND);
  } else {
    console.log("🚀 Starting staggered realistic testing...");
    devices.forEach((device, index) => {
      const staggerDelay = index * 500;
      setTimeout(() => {
        console.log(`🟢 Device ${device} started`);
        setInterval(async () => {
          try {
            const r = await publishTelemetry(device);
            console.log(`📤 ${device} sent | power=${r.power} | publish=${r.publishLatency}ms`);
          } catch (err) {
            console.error(`❌ [${device}] Failed:`, err);
          }
        }, TIME_TO_SEND);
      }, staggerDelay);
    });
  }
}

// -------------------- MQTT Client --------------------
const client = mqtt.connect(connectionConfig);

client.on("connect", () => {
  console.log(`✅ Connected to ${LOCAL ? "EMQX" : "AWS IoT Core"} broker`);
  console.log(`Starting load test with ${NUM_DEVICES} devices`);
  console.log(`🔄 Mode: ${syncTesting ? "SYNCHRONIZED (burst)" : "CONCURRENT (staggered)"}`);
  console.log(`📌 Topic: ${MQTT_TOPIC}`);
  if (LOCAL) console.log("📊 EMQX Dashboard: http://localhost:18083 (admin/public)");
  startLoadTest();
});

client.on("error", (err) => console.error("❌ MQTT error:", err));
client.on("reconnect", () => console.log("🔄 MQTT reconnecting..."));
client.on("offline", () => console.log("⚠️ MQTT offline"));
client.on("close", () => console.log("🔌 MQTT closed"));

// Graceful shutdown
process.on("SIGINT", () => {
  console.log("\nShutting down...");
  client.end();
  process.exit(0);
});
