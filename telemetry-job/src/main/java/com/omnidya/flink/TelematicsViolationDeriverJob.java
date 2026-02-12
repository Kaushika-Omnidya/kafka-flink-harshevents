package com.omnidya.flink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Telematics Violation Deriver Job (Option A)
 *
 * INPUT : Kafka topic telemetry.raw
 * OUTPUT:
 *   - violations.events        (one message per violation)
 *   - device-status.events     (touch/clear cable-unplugged)
 *
 * Raw telemetry is NOT copied to telemetry.processed.
 */
public class TelematicsViolationDeriverJob {

    // Avoid Kafka warning: don't mix '.' and '_' together in topic names
    private static final String INPUT_TOPIC = "telemetry.raw";
    private static final String VIOLATIONS_TOPIC = "violations.events";
    private static final String STATUS_TOPIC = "device-status.events";

    // inside docker network use kafka:9094 (your setup)
    private static final String BOOTSTRAP = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "kafka:9094");

    // side outputs
    private static final OutputTag<String> VIOLATIONS_OUT = new OutputTag<String>("violations-out") {};
    private static final OutputTag<String> STATUS_OUT = new OutputTag<String>("status-out") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP)
                .setTopics(INPUT_TOPIC)
                .setGroupId("flink-violation-deriver")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSink<String> violationsSink = KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(VIOLATIONS_TOPIC)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();

        KafkaSink<String> statusSink = KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(STATUS_TOPIC)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();

        DataStream<String> raw = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");

        SingleOutputStreamOperator<String> main = raw.process(new DeriveEventsProcess())
                .name("derive-violation-and-status-events");

        DataStream<String> violationsStream = main.getSideOutput(VIOLATIONS_OUT);
        DataStream<String> statusStream = main.getSideOutput(STATUS_OUT);

        // write both streams
        violationsStream.sinkTo(violationsSink).name("kafka-sink-violations-events");
        statusStream.sinkTo(statusSink).name("kafka-sink-device-status-events");

        env.execute("telematics-violation-deriver-job");
    }

    static class DeriveEventsProcess extends ProcessFunction<String, String> {

        private transient ObjectMapper mapper;

        // Keep only allowed types (same concept as mongo.is_valid_violation_type())
        private final Set<String> allowedViolationTypes = new HashSet<String>() {{
            add("harsh_brake");
            add("harsh_accel");
            // add more later: geo_violation, overspeed, etc.
        }};

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) {
            if (mapper == null) {
                mapper = new ObjectMapper();
                mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            }

            ObjectNode payload = safeParseObject(value, mapper);
            if (payload == null) {
                return;
            }

            // Common fields
            String deviceUuid = text(payload, "device_uuid");
            long ts = longVal(payload, "timestamp");
            String vehicleId = text(payload, "vehicle_id");
            String accountId = text(payload, "account_id");
            String powerSource = text(payload, "dashcam_power_source");

            // 1) STATUS EVENTS (emit only when power source is battery)
            if (deviceUuid != null && ts > 0) {
                if ("battery".equalsIgnoreCase(powerSource)) {
                    // touch event (session logic happens in Node writer with Redis+Mongo)
                    ObjectNode ev = mapper.createObjectNode();
                    ev.put("event_type", "device_status");
                    ev.put("status_type", "cable-unplugged");
                    ev.put("action", "touch"); // meaning: extend/update session
                    ev.put("device_uuid", deviceUuid);
                    ev.put("timestamp", ts);
                    if (vehicleId != null) ev.put("vehicle_id", vehicleId);
                    if (accountId != null) ev.put("account_id", accountId);

                    // include location if exists
                    if (payload.has("location")) ev.set("location", payload.get("location"));

                    ctx.output(STATUS_OUT, ev.toString());
                }
            }

            // 2) VIOLATION EVENTS (one output per violation)
            if (!payload.has("violations") || !payload.get("violations").isArray()) return;

            Iterator<com.fasterxml.jackson.databind.JsonNode> it = payload.get("violations").elements();
            while (it.hasNext()) {
                com.fasterxml.jackson.databind.JsonNode v = it.next();
                if (!v.isObject()) continue;

                String type = v.has("type") ? v.get("type").asText() : "";
                if (!allowedViolationTypes.contains(type)) {
                    continue;
                }

                ObjectNode ev = mapper.createObjectNode();
                ev.put("event_type", "violation");
                ev.put("violation_type", type);

                if (deviceUuid != null) ev.put("device_uuid", deviceUuid);
                if (vehicleId != null) ev.put("vehicle_id", vehicleId);
                if (accountId != null) ev.put("account_id", accountId);

                // event timestamp: prefer violation timestamp, else payload timestamp
                long vts = (v.has("timestamp") ? v.get("timestamp").asLong() : 0L);
                ev.put("timestamp", vts > 0 ? vts : ts);

                // copy location if exists
                if (payload.has("location")) ev.set("location", payload.get("location"));

                // store violation details under "details"
                ObjectNode details = mapper.createObjectNode();
                if (v.has("accel_y")) details.put("accel_y", v.get("accel_y").asDouble());
                if (v.has("speed_kph")) details.put("speed_kph", v.get("speed_kph").asDouble());
                if (v.has("delta_speed")) details.put("delta_speed", v.get("delta_speed").asDouble());
                ev.set("details", details);

                // optional: pass through mqtt_sent_at_ms for latency correlation
                if (payload.has("mqtt_sent_at_ms")) ev.put("mqtt_sent_at_ms", payload.get("mqtt_sent_at_ms").asLong());

                ctx.output(VIOLATIONS_OUT, ev.toString());
            }
        }

        private static ObjectNode safeParseObject(String raw, ObjectMapper mapper) {
            try {
                if (raw == null) return null;
                String s = raw.trim();
                if (s.isEmpty()) return null;

                // handle double-encoded JSON string
                if (s.startsWith("\"") && s.endsWith("\"")) {
                    s = s.substring(1, s.length() - 1)
                            .replace("\\\"", "\"")
                            .replace("\\\\", "\\");
                }

                com.fasterxml.jackson.databind.JsonNode node = mapper.readTree(s);
                if (node != null && node.isObject()) {
                    return (ObjectNode) node;
                }
                return null;
            } catch (JsonProcessingException e) {
                return null;
            }
        }

        private static String text(ObjectNode o, String field) {
            return (o.has(field) && !o.get(field).isNull()) ? o.get(field).asText() : null;
        }

        private static long longVal(ObjectNode o, String field) {
            return (o.has(field) && o.get(field).canConvertToLong()) ? o.get(field).asLong() : 0L;
        }
    }
}
