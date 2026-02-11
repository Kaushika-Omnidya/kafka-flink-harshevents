const { Kafka } = require('kafkajs');
const mongoose = require('mongoose');
const redis = require('redis');
const fs = require('fs').promises;
const path = require('path');

const kafka = new Kafka({
  clientId: 'db-writer',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'flink-to-db' });

// MongoDB Connection
const MONGO_URI = 'mongodb://localhost:27017/das';

mongoose.connect(MONGO_URI)
  .then(() => console.log('MongoDB connected successfully'))
  .catch(err => console.error('MongoDB connection error:', err));

// Redis Connection
const redisClient = redis.createClient({
  url: process.env.REDIS_URL || 'redis://localhost:6379'
});

redisClient.on('error', (err) => console.error('Redis error:', err));
redisClient.on('connect', () => console.log('Redis connected successfully'));

(async () => {
  await redisClient.connect();
})();

// Location Telemetry Schema
const locationTelemetrySchema = new mongoose.Schema({
  timestamp: { type: Number, required: true, index: true },
  device_uuid: { type: String, required: true, index: true },
  vehicle_id: { type: String, required: true, index: true },
  account_id: { type: String, required: true, index: true },

  // Location data
  recent_location: {
    type: { type: String, enum: ['Point'], default: 'Point' },
    coordinates: { type: [Number], required: true }
  },
  previous_location: {
    type: { type: String, enum: ['Point'], default: 'Point' },
    coordinates: { type: [Number], required: true }
  },
  lat_dir: String,
  lon_dir: String,
  location_changed: Boolean,
  speed_kph: Number,
  speed_mph: Number,

  // Trip and violations
  ontrip: Boolean,
  violations: [String],

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
  battery_capacity: Number
}, {
  timestamps: true,
  collection: 'live-location-telemetry'
});

// Create geospatial indexes
locationTelemetrySchema.index({ recent_location: '2dsphere' });
locationTelemetrySchema.index({ previous_location: '2dsphere' });

// Compound indexes for common queries
locationTelemetrySchema.index({ vehicle_id: 1, timestamp: -1 });
locationTelemetrySchema.index({ device_uuid: 1, timestamp: -1 });
locationTelemetrySchema.index({ account_id: 1, timestamp: -1 });

const LocationTelemetry = mongoose.model('LocationTelemetry', locationTelemetrySchema);

// Redis key prefix for device document IDs
const DEVICE_DOC_PREFIX = 'device:doc:';

// Latency tracking
const LATENCY_FILE = path.join(__dirname, 'latencies.json');
const latencyBuffer = [];
const LATENCY_FLUSH_INTERVAL = 5000; // Write to file every 5 seconds

// Initialize latency file
async function initLatencyFile() {
  try {
    await fs.access(LATENCY_FILE);
    console.log('Latency file exists, will append to it');
  } catch {
    // File doesn't exist, create it with empty array
    await fs.writeFile(LATENCY_FILE, '[]', 'utf8');
    console.log('📊 Created latencies.json file');
  }
}

// Append latencies to file
async function flushLatencies() {
  if (latencyBuffer.length === 0) return;

  try {
    // Read existing data
    const fileContent = await fs.readFile(LATENCY_FILE, 'utf8');
    const existingLatencies = JSON.parse(fileContent);

    // Append new latencies
    existingLatencies.push(...latencyBuffer);

    // Write back to file
    await fs.writeFile(LATENCY_FILE, JSON.stringify(existingLatencies, null, 2), 'utf8');

    console.log(`📊 Flushed ${latencyBuffer.length} latency records to file (Total: ${existingLatencies.length})`);
    latencyBuffer.length = 0; // Clear buffer
  } catch (error) {
    console.error('❌ Error writing latencies to file:', error);
  }
}

// Periodically flush latencies to file
setInterval(flushLatencies, LATENCY_FLUSH_INTERVAL);

// Kafka Consumer Logic
const run = async () => {
  await consumer.connect();
  console.log('Kafka consumer connected');

  // Initialize latency tracking file
  await initLatencyFile();

  await consumer.subscribe({
    topic: 'telemetry.processed',
    fromBeginning: false
  });

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const receivedAt = Date.now();
        const telemetryData = JSON.parse(message.value.toString());
        const deviceUuid = telemetryData.device_uuid;

        // Calculate end-to-end latency
        const latency = receivedAt - telemetryData.mqtt_sent_at_ms;

        // Store latency data
        latencyBuffer.push({
          device_uuid: deviceUuid,
          mqtt_sent_at_ms: telemetryData.mqtt_sent_at_ms,
          kafka_received_at_ms: receivedAt,
          latency_ms: latency,
          timestamp: new Date(receivedAt).toISOString()
        });

        console.log(`Received telemetry from ${deviceUuid} at ${new Date(telemetryData.timestamp * 1000).toISOString()} | Latency: ${latency}ms`);

        // Check Redis cache for existing document ID
        const cacheKey = `${DEVICE_DOC_PREFIX}${deviceUuid}`;
        let docId = await redisClient.get(cacheKey);

        if (docId) {
          // Device exists in cache - directly update by ID (fastest)
          await LocationTelemetry.findByIdAndUpdate(
            docId,
            telemetryData,
            { new: true }
          );
          console.log(`✓ [CACHED] Updated ${deviceUuid} | Vehicle: ${telemetryData.vehicle_id} | Speed: ${telemetryData.speed_mph} mph | Battery: ${telemetryData.battery_capacity}%`);
        } else {
          // Device not in cache - find or create, then cache the ID
          const doc = await LocationTelemetry.findOneAndUpdate(
            { device_uuid: deviceUuid },
            telemetryData,
            {
              upsert: true,
              new: true,
              setDefaultsOnInsert: true
            }
          );

          // Cache the document ID in Redis (no expiration - permanent cache)
          await redisClient.set(cacheKey, doc._id.toString());
          console.log(`✓ [NEW] Cached ${deviceUuid} | Vehicle: ${telemetryData.vehicle_id} | Speed: ${telemetryData.speed_mph} mph | Battery: ${telemetryData.battery_capacity}%`);
        }

      } catch (error) {
        console.error('Error processing message:', error);
      }
    },
  });
};

// Error handling
consumer.on('consumer.crash', async (event) => {
  console.error('Consumer crashed:', event.payload.error);
  await run();
});

consumer.on('consumer.disconnect', () => {
  console.log('Consumer disconnected');
});

// Graceful shutdown
const shutdown = async () => {
  console.log('\nShutting down gracefully...');

  // Flush any remaining latencies before shutdown
  await flushLatencies();

  await consumer.disconnect();
  await mongoose.connection.close();
  await redisClient.quit();
  console.log('All connections closed');
  process.exit(0);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

// Start the consumer
run().catch(console.error);

