require('dotenv').config();
const mqtt = require('mqtt');
const fs = require('fs');
const path = require('path');

const LOCAL = true;

// CONFIG
const NUM_DEVICES = 1;


let connectionConfig;

const syncTesting = true;
const TIME_TO_SEND = 1000;

// Latency tracking
const latencyStats = {
  publishLatencies: [],

  addPublishLatency(latency) {
    this.publishLatencies.push(latency);
    // Keep only last 1000 samples to avoid memory issues
    if (this.publishLatencies.length > 1000) {
      this.publishLatencies.shift();
    }
  },

  getStats(samples) {
    if (samples.length === 0) return null;

    const sorted = [...samples].sort((a, b) => a - b);
    const sum = sorted.reduce((a, b) => a + b, 0);

    return {
      count: sorted.length,
      min: sorted[0],
      max: sorted[sorted.length - 1],
      avg: sum / sorted.length,
      p50: sorted[Math.floor(sorted.length * 0.5)],
      p95: sorted[Math.floor(sorted.length * 0.95)],
      p99: sorted[Math.floor(sorted.length * 0.99)]
    };
  },

  printStats() {
    console.log('\n' + '='.repeat(60));
    console.log('📊 LATENCY STATISTICS');
    console.log('='.repeat(60));

    const pubStats = this.getStats(this.publishLatencies);

    if (pubStats) {
      console.log('\n🚀 Publish Latency (time to broker acknowledgment):');
      console.log(`  Samples: ${pubStats.count}`);
      console.log(`  Min:     ${pubStats.min.toFixed(2)}ms`);
      console.log(`  Avg:     ${pubStats.avg.toFixed(2)}ms`);
      console.log(`  P50:     ${pubStats.p50.toFixed(2)}ms`);
      console.log(`  P95:     ${pubStats.p95.toFixed(2)}ms`);
      console.log(`  P99:     ${pubStats.p99.toFixed(2)}ms`);
      console.log(`  Max:     ${pubStats.max.toFixed(2)}ms`);
    } else {
      console.log('\n⚠️  No latency data collected yet');
    }

    console.log('\n' + '='.repeat(60) + '\n');
  }
};

// Print latency stats every 30 seconds
setInterval(() => {
  latencyStats.printStats();
}, 30000);

function generateDevices(n){

    const deviceUUIDs = [];

    for (let i = 1; i <=n; i++){
        deviceUUIDs.push(`device-${i}`);
    }
    return deviceUUIDs;
}

let devices = generateDevices(NUM_DEVICES);

if(LOCAL){
    MQTT_TOPIC = 'test/topic';
    // EMQX local connection (docker-compose)
    connectionConfig ={
    host: 'localhost',
    port: parseInt('1883'),
    protocol: 'mqtt',
    clientId: `loadtest_publisher_${Math.random().toString(16).substring(2, 10)}`,
    keepalive: 60,
    reconnectPeriod: 1000,
    connectTimeout: 30000,
    clean: true
    };
} else {
  const AWS_IOT_ENDPOINT = process.env.AWS_IOT_ENDPOINT;
  const AWS_CLIENT_ID = process.env.AWS_CLIENT_ID;
  const AWS_CERT_PATH = process.env.AWS_CERT_PATH;
  const AWS_KEY_PATH = process.env.AWS_KEY_PATH;
  const AWS_CA_PATH = process.env.AWS_CA_PATH;

  MQTT_TOPIC = 'telematics/+'

  if (!AWS_IOT_ENDPOINT || !AWS_CLIENT_ID || !AWS_CERT_PATH || !AWS_KEY_PATH || !AWS_CA_PATH) {
    console.error('❌ Missing AWS IoT configuration in .env file');
    console.error('Required variables:');
    console.error(`  AWS_IOT_ENDPOINT: ${AWS_IOT_ENDPOINT ? '✓' : '✗ MISSING'}`);
    console.error(`  AWS_CLIENT_ID: ${AWS_CLIENT_ID ? '✓' : '✗ MISSING'}`);
    console.error(`  AWS_CERT_PATH: ${AWS_CERT_PATH ? '✓' : '✗ MISSING'}`);
    console.error(`  AWS_KEY_PATH: ${AWS_KEY_PATH ? '✓' : '✗ MISSING'}`);
    console.error(`  AWS_CA_PATH: ${AWS_CA_PATH ? '✓' : '✗ MISSING'}`);
    process.exit(1);
  }
  connectionConfig = {
      host: AWS_IOT_ENDPOINT,
      port: 8883,
      protocol: 'mqtts',
      clientId: `${AWS_CLIENT_ID}_loadtest_${Math.random().toString(16).substring(2, 10)}`,
      cert: fs.readFileSync(path.join(__dirname, AWS_CERT_PATH)),
      key: fs.readFileSync(path.join(__dirname, AWS_KEY_PATH)),
      ca: fs.readFileSync(path.join(__dirname, AWS_CA_PATH)),
      rejectUnauthorized: true,
      keepalive: 60,
      reconnectPeriod: 1000,
      connectTimeout: 30000,
      clean: true
    };
}

// MQTT connection
const client = mqtt.connect(connectionConfig);

client.on('connect', () => {
  console.log(`✅ Connected to ${LOCAL ? 'EMQX' : 'AWS IoT Core'} broker`);
  console.log(`Starting load test with ${NUM_DEVICES} devices`);
  console.log(`🔄 Mode: ${syncTesting ? 'SYNCHRONIZED (burst)' : 'CONCURRENT (staggered)'}`);
  if (LOCAL) {
    console.log(`📊 EMQX Dashboard: http://localhost:18083 (admin/public)`);
  }
  startLoadTest();
});

client.on('error', (err) => {
  console.error('❌ MQTT connection error:', err);
});

client.on('reconnect', () => {
  console.log('🔄 Reconnecting to MQTT broker...');
});

client.on('offline', () => {
  console.log('⚠️  MQTT client offline');
});

client.on('close', () => {
  console.log('🔌 MQTT connection closed');
});

function generateTelemetryData(deviceUuid) {
  const fixQualityOptions = ['0', '1', '2'];
  const powerSourceOptions = ['battery', 'external', 'usb', 'vehicle'];

  return {
    device_uuid: deviceUuid,
    mqtt_sent_at_ms: Date.now(), // Millisecond timestamp for latency tracking
    timestamp: Math.floor(Date.now() / 1000), // Unix timestamp in seconds
    fix_quality: fixQualityOptions[Math.floor(Math.random() * fixQualityOptions.length)],
    temp_C: parseFloat((20 + Math.random() * 15).toFixed(2)),
    accel_x: parseFloat(((Math.random() - 0.5) * 2).toFixed(6)),
    accel_y: parseFloat(((Math.random() - 0.5) * 2).toFixed(6)),
    accel_z: parseFloat((9.8 + (Math.random() - 0.5) * 0.2).toFixed(6)),
    gyro_x: parseFloat(((Math.random() - 0.5) * 50).toFixed(6)),
    gyro_y: parseFloat(((Math.random() - 0.5) * 50).toFixed(6)),
    gyro_z: parseFloat(((Math.random() - 0.5) * 50).toFixed(6)),
    cpu_temp: Math.floor(50 + Math.random() * 20), // Int32
    soc_temp: Math.floor(55 + Math.random() * 15), // Int32
    main_board_temp: parseFloat((45 + Math.random() * 10).toFixed(2)),
    sim_iccid: `8901234567890123${String(Math.floor(Math.random() * 1000)).padStart(3, '0')}`,
    sim_imsi: `310120123456${String(Math.floor(Math.random() * 1000)).padStart(3, '0')}`,
    signal_strength_percent: Math.floor(50 + Math.random() * 50), // Int32
    imu_is_stopped: Math.random() > 0.8,
    dashcam_power_source: powerSourceOptions[Math.floor(Math.random() * powerSourceOptions.length)],
    battery_capacity: Math.floor(50 + Math.random() * 50), // Int32
    lat_dir: 'N',
    lon_dir: 'W',
    location_changed: Math.random() > 0.7,
    speed_kph: parseFloat((Math.random() * 120).toFixed(2)),
    speed_mph: parseFloat((Math.random() * 75).toFixed(2)),
    ontrip: Math.random() > 0.3,
    location: {
      type: 'Point',
      coordinates: [
        parseFloat((-122.4194 + (Math.random() - 0.5) * 0.1).toFixed(6)), // lon (SF area)
        parseFloat((37.7749 + (Math.random() - 0.5) * 0.1).toFixed(6))    // lat
      ]
    },
    vehicle_id: `vehicle-${String(Math.floor(Math.random() * 100)).padStart(3, '0')}`,
    account_id: `account-${String(Math.floor(Math.random() * 20)).padStart(2, '0')}`,
    violations: []
  };
}


async function publishTelemetry(device) {
  return new Promise((resolve, reject) => {
    const telemetry = generateTelemetryData(device);
    const topic = 'test/topic';
    const publishStartTime = Date.now();

    
    client.publish(topic, JSON.stringify(telemetry), { qos: 0 }, (err) => {
      const publishLatency = Date.now() - publishStartTime;

      if (err) {
        reject(err);
      } else {
        // Track publish latency
        latencyStats.addPublishLatency(publishLatency);
        resolve({ device, publishLatency });
      }
    });
  });
}

async function sendBurst() {
  const timestamp = new Date().toISOString();

  try {
    // Send all device messages concurrently
    const results = await Promise.all(devices.map(device => publishTelemetry(device)));
    const avgPublishLatency = results.reduce((sum, r) => sum + r.publishLatency, 0) / results.length;
    console.log(`📤 [${timestamp}] All ${NUM_DEVICES} devices sent (avg publish latency: ${avgPublishLatency.toFixed(2)}ms)`);
  } catch (err) {
    console.error(`❌ Error during burst:`, err);
  }
}

function startLoadTest() {
  if (syncTesting) {
    // SYNCHRONIZED: All devices slam MQTT at once
    console.log('🚀 Starting synchronized burst testing...');

    setInterval(async () => {
      await sendBurst();
    }, TIME_TO_SEND);

  } else {
    // STAGGERED/REALISTIC: Each device runs independently
    console.log('🚀 Starting staggered realistic testing...');

    devices.forEach((device, index) => {
      const staggerDelay = index * 500; // 500ms stagger between starts

      setTimeout(() => {
        console.log(`🟢 Device ${device} started`);

        // Each device has independent 5-second interval
        setInterval(async () => {
          const timestamp = new Date().toISOString();

          try {
            const result = await publishTelemetry(device);
            console.log(`📤 [${timestamp}] ${device} sent (publish latency: ${result.publishLatency}ms)`);
          } catch (err) {
            console.error(`❌ [${device}] Failed:`, err);
          }
        }, TIME_TO_SEND);

      }, staggerDelay);
    });
  }
}

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('\nShutting down load test...');
  client.end();
  process.exit(0);
});