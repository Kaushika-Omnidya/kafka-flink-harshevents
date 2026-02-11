# Kafka-Flink Telemetry Pipeline

A real-time telemetry data pipeline that ingests MQTT messages, processes them through Kafka and Apache Flink, and stores them in MongoDB with Redis caching. Includes latency tracking and visualization capabilities.

## Architecture

```text
MQTT Publishers → EMQX Broker → MQTT-Kafka Bridge → Kafka (telemetry.raw)
                                                          ↓
                                                    Flink Processing
                                                          ↓
                                                  Kafka (telemetry.processed)
                                                          ↓
                                                  Kafka Consumer → MongoDB + Redis
```

## Components

- **EMQX**: MQTT broker for receiving telemetry data
- **Kafka**: Message streaming platform with two topics:
  - `telemetry.raw`: Raw telemetry data from MQTT
  - `telemetry.processed`: Processed data from Flink
- **Apache Flink**: Stream processing framework (jobs deployed separately)
- **MongoDB**: Primary data store with geospatial indexes
- **Redis**: Caching layer for device document IDs
- **Node.js Services**:
  - MQTT publisher for load testing
  - MQTT-to-Kafka bridge
  - Kafka consumer with database writer
  - Python visualization for latency analysis

## Prerequisites

- Docker and Docker Compose
- Node.js (v16+)
- Python 3.x (for visualization)
- MongoDB running on `localhost:27017` (or update connection string)
- Redis running on `localhost:6379` (or update connection string)
- Java 17 (for building Flink jobs)
- Maven (for building Flink jobs)

## Database Setup

### MongoDB with Docker

#### 1. Run MongoDB Container

Start MongoDB with persistent storage:

```bash
docker run -d \
  --name mongodb \
  -p 27017:27017 \
  -v mongodb_data:/data/db \
  -e MONGO_INITDB_DATABASE=das \
  mongo:latest
```

**Configuration:**
- **Port**: `27017` (default MongoDB port)
- **Volume**: `mongodb_data` for persistent storage
- **Database**: `das` (initialized on first run)

#### 2. Verify MongoDB is Running

```bash
# Check container status
docker ps | grep mongodb

# View logs
docker logs mongodb

# Test connection
docker exec -it mongodb mongosh --eval "db.adminCommand('ping')"
```

#### 3. Connect with MongoDB Compass

MongoDB Compass is the official GUI for MongoDB. [Download it here](https://www.mongodb.com/products/compass).

**Connection Steps:**

1. Open MongoDB Compass
2. Use the connection string:

   ```text
   mongodb://localhost:27017/das
   ```

3. Click "Connect"

**Managing Data:**

- Browse collections: Navigate to the `das` database
- View telemetry data: Check the `telemetry` collection
- Indexes: The system automatically creates a `2dsphere` index on `location` field
- Query: Use the filter bar to query telemetry data (e.g., `{device_uuid: "device-1"}`)

#### 4. Useful MongoDB Commands

```bash
# Access MongoDB shell
docker exec -it mongodb mongosh

# Inside mongosh:
use das                          # Switch to das database
db.telemetry.countDocuments()    # Count telemetry records
db.telemetry.find().limit(5)    # View first 5 records
db.telemetry.getIndexes()       # View indexes
db.telemetry.drop()              # Clear collection (use with caution)
```

#### 5. Stop and Remove MongoDB

```bash
# Stop container
docker stop mongodb

# Remove container (data persists in volume)
docker rm mongodb

# Remove container AND data volume (WARNING: deletes all data)
docker rm mongodb && docker volume rm mongodb_data
```

### Redis with Docker

#### 1. Run Redis Container

Start Redis with persistent storage:

```bash
docker run -d \
  --name redis \
  -p 6379:6379 \
  -v redis_data:/data \
  redis:latest redis-server --appendonly yes
```

**Configuration:**
- **Port**: `6379` (default Redis port)
- **Volume**: `redis_data` for persistent storage
- **AOF Persistence**: `--appendonly yes` enables append-only file for durability

#### 2. Verify Redis is Running

```bash
# Check container status
docker ps | grep redis

# View logs
docker logs redis

# Test connection
docker exec -it redis redis-cli ping
# Should return: PONG
```

#### 3. Monitor Redis Cache

The Kafka consumer uses Redis to cache device document IDs to avoid duplicate MongoDB lookups.

**Connect to Redis CLI:**

```bash
docker exec -it redis redis-cli
```

**Useful Redis commands:**

```bash
# View all cached device IDs
KEYS device:*

# Get cached MongoDB ObjectId for a device
GET device:device-1

# Count cached devices
DBSIZE

# Monitor real-time commands (useful for debugging)
MONITOR

# View memory usage
INFO memory

# Clear all cache (use with caution)
FLUSHALL
```

#### 4. Redis Persistence Options

Redis supports two persistence modes:

**AOF (Append-Only File)** - Current setup:
```bash
docker run -d --name redis -p 6379:6379 \
  -v redis_data:/data \
  redis:latest redis-server --appendonly yes
```

**RDB (Snapshot)** - Alternative:
```bash
docker run -d --name redis -p 6379:6379 \
  -v redis_data:/data \
  redis:latest redis-server --save 60 1
```

#### 5. Stop and Remove Redis

```bash
# Stop container
docker stop redis

# Remove container (data persists in volume)
docker rm redis

# Remove container AND data volume (WARNING: deletes all data)
docker rm redis && docker volume rm redis_data
```

### Combined Setup (Both Databases)

Run both databases with a single docker-compose file:

```yaml
# database-compose.yml
version: '3.8'

services:
  mongodb:
    image: mongo:latest
    container_name: mongodb
    ports:
      - "27017:27017"
    volumes:
      - mongodb_data:/data/db
    environment:
      - MONGO_INITDB_DATABASE=das

  redis:
    image: redis:latest
    container_name: redis
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    command: redis-server --appendonly yes

volumes:
  mongodb_data:
  redis_data:
```

**Usage:**

```bash
# Start both databases
docker-compose -f database-compose.yml up -d

# Stop both databases
docker-compose -f database-compose.yml down

# Stop and remove data volumes
docker-compose -f database-compose.yml down -v
```

## Quick Start

### 1. Start Infrastructure

```bash
docker-compose up -d
```

This starts:

- Kafka on ports `9092` (external) and `9094` (internal)
- EMQX on ports `1883` (MQTT), `8083` (WebSocket), `18083` (Dashboard)
- Flink JobManager on port `8081`
- Flink TaskManager

### 2. Create Kafka Topics

Create the required Kafka topics with proper configuration:

```bash
# Create raw telemetry topic
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic telemetry.raw \
  --partitions 6 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000

# Create processed telemetry topic
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic telemetry.processed \
  --partitions 6 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000

# Verify topics were created
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

**Topic Configuration:**

- **Partitions**: 6 (for parallel processing)
- **Replication Factor**: 1 (single broker setup)
- **Retention**: 7 days (604800000ms)
- **Cleanup Policy**: Delete old messages

### 3. Install Node.js Dependencies

```bash
npm install
```

### 4. Build Flink Job (Optional)

The Flink processing job is included as `telemetry-job.zip`. To build it:

```bash
# Unzip the Flink job in a separate directory
unzip telemetry-job.zip

# Open in another VSCode instance and navigate to the project
cd telemetry-job

# Compile and package using Maven
mvn clean package
```

**Requirements:**

- Java 17
- Maven

This will generate the JAR file in the `target/` directory.

### 5. Deploy Flink Job (Optional)

If you have a compiled Flink job:

```bash
# Copy JAR to Flink JobManager
docker cp target/telemetry-job-1.0-SNAPSHOT.jar flink-jobmanager:/job.jar

# Submit job
docker exec -it flink-jobmanager flink run /job.jar
```

### 6. Start Services

Start services in this order:

```bash
# Terminal 1: MQTT-to-Kafka Bridge
node mqttToKafka.js

# Terminal 2: Kafka Consumer (writes to MongoDB/Redis)
node kafkaConsumer.js

# Terminal 3: MQTT Publisher (for testing)
node mqtt_publish.js
```

## Configuration

### MQTT Publisher ([mqtt_publish.js](mqtt_publish.js))

Edit configuration at the top of the file:

```javascript
const LOCAL = true;              // Use local EMQX or AWS IoT Core
const NUM_DEVICES = 1;           // Number of simulated devices
const syncTesting = true;        // Synchronized bursts vs staggered
const TIME_TO_SEND = 1000;       // Interval in milliseconds
```

For AWS IoT Core, create a `.env` file:

```env
AWS_IOT_ENDPOINT=your-endpoint.iot.region.amazonaws.com
AWS_CLIENT_ID=your-client-id
AWS_CERT_PATH=./certs/certificate.pem.crt
AWS_KEY_PATH=./certs/private.pem.key
AWS_CA_PATH=./certs/AmazonRootCA1.pem
```

### Database Connections ([kafkaConsumer.js](kafkaConsumer.js))

Default connection strings:

- MongoDB: `mongodb://localhost:27017/das`
- Redis: `redis://localhost:6379`

Override Redis via environment variable:

```bash
export REDIS_URL=redis://your-redis-host:6379
```

## Monitoring & Dashboards

- **EMQX Dashboard**: <http://localhost:18083> (admin/public)
- **Flink Dashboard**: <http://localhost:8081>
- **Latency Data**: Automatically saved to [latencies.json](latencies.json)

## Consuming Messages Manually

Monitor Kafka topics directly:

```bash
# Watch raw telemetry
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic telemetry.raw \
  --from-beginning

# Watch processed telemetry
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic telemetry.processed \
  --from-beginning
```

## Data Schema

### Telemetry Message Structure

```json
{
  "device_uuid": "device-1",
  "mqtt_sent_at_ms": 1234567890123,
  "timestamp": 1234567890,
  "fix_quality": "1",
  "temp_C": 25.5,
  "accel_x": 0.5,
  "accel_y": -0.3,
  "accel_z": 9.8,
  "gyro_x": 1.2,
  "gyro_y": -0.8,
  "gyro_z": 0.3,
  "cpu_temp": 55,
  "soc_temp": 60,
  "main_board_temp": 48.5,
  "sim_iccid": "89012345678901234567",
  "sim_imsi": "310120123456789",
  "signal_strength_percent": 85,
  "imu_is_stopped": false,
  "dashcam_power_source": "vehicle",
  "battery_capacity": 75,
  "lat_dir": "N",
  "lon_dir": "W",
  "location_changed": true,
  "speed_kph": 65.5,
  "speed_mph": 40.7,
  "ontrip": true,
  "location": {
    "type": "Point",
    "coordinates": [-122.4194, 37.7749]
  },
  "vehicle_id": "vehicle-001",
  "account_id": "account-01",
  "violations": []
}
```

## Performance & Latency Tracking

The system tracks end-to-end latency from MQTT publish to database write:

- **Publish Latency**: MQTT broker acknowledgment time
- **End-to-End Latency**: MQTT send to Kafka consumer receive
- **Statistics**: Min, Max, Avg, P50, P95, P99

Latency data is automatically saved to `latencies.json` every 5 seconds.

### Visualize Latency

```bash
python visualization.py
```

Generates `latency_graph.png` with latency distribution.

## Troubleshooting

### Kafka Connection Issues

```bash
# Check if Kafka is running
docker ps | grep kafka

# View Kafka logs
docker logs kafka

# Test connection
docker exec -it kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092
```

### EMQX Connection Issues

```bash
# Check EMQX status
docker logs emqx

# Test MQTT connection
npm install -g mqtt
mqtt pub -h localhost -p 1883 -t test/topic -m "test message"
```

### MongoDB/Redis Not Found

Ensure MongoDB and Redis are running locally:

```bash
# MongoDB
mongod --version
sudo systemctl status mongod  # or brew services list

# Redis
redis-cli ping
sudo systemctl status redis    # or brew services list
```

## Cleanup

```bash
# Stop all containers
docker-compose down

# Remove volumes (WARNING: deletes all data)
docker-compose down -v

# Clean up latency data
rm latencies.json latency_graph.png
```

## Project Structure

```text
.
├── docker-compose.yml          # Infrastructure definition
├── package.json                # Node.js dependencies
├── mqtt_publish.js             # MQTT load test publisher
├── mqttToKafka.js             # MQTT → Kafka bridge
├── kafkaConsumer.js           # Kafka → MongoDB/Redis consumer
├── visualization.py            # Latency visualization
├── latencies.json             # Latency tracking data
└── kafka-demo/                # Additional demo files
```

## License

MIT
