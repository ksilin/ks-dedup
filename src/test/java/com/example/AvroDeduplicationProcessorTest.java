package com.example;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AvroDeduplicationProcessorTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, GenericRecord> inputTopic;
    private TestOutputTopic<String, GenericRecord> outputTopic;

    private final String INPUT_TOPIC = "input-topic";
    private final String OUTPUT_TOPIC = "output-topic";
    private final String storeName = "test-store";

    private final Map<String, String> serdeConfig = Collections.singletonMap(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://test-schema-registry");

    private final Serde<String> stringSerde = Serdes.String();
    private final Serde<GenericRecord> avroSerde = new GenericAvroSerde();

    private final String user1Key = "user1";
    private final String user2Key = "user2";

    private final String schemaString = "{\n" +
            "   \"type\": \"record\",\n" +
            "   \"name\": \"User\",\n" +
            "   \"fields\": [\n" +
            "       {\"name\": \"name\", \"type\": \"string\"},\n" +
            "       {\"name\": \"age\", \"type\": \"int\"}\n" +
            "   ]\n" +
            "}";

    private final Schema schema = new Schema.Parser().parse(schemaString);

    @BeforeEach
    public void setup() {
        avroSerde.configure(serdeConfig, false);
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ks-dedup");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        testDriver = new TopologyTestDriver(createTestTopology(), props);

        inputTopic = testDriver.createInputTopic(INPUT_TOPIC, stringSerde.serializer(), avroSerde.serializer());
        outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringSerde.deserializer(), avroSerde.deserializer());
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    Topology createTestTopology() {
        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(storeName);
        StoreBuilder<KeyValueStore<String, String>> storeBuilder =
                Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.String());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.addStateStore(storeBuilder);

        streamsBuilder.stream(INPUT_TOPIC, Consumed.with(stringSerde, avroSerde))
                .processValues(() -> new AvroDeduplicationProcessor(storeName), storeName)
                .to(OUTPUT_TOPIC, Produced.with(stringSerde, avroSerde));

        return streamsBuilder.build();
    }

    @Test
    public void testDeduplicationOfDuplicates() {
        GenericRecord record1 = new GenericData.Record(schema);
        record1.put("name", "Alice");
        record1.put("age", 30);

        GenericRecord record2 = new GenericData.Record(schema);
        record2.put("name", "Alice");
        record2.put("age", 30);

        inputTopic.pipeInput(user1Key, record1);
        inputTopic.pipeInput(user1Key, record2);

        List<KeyValue<String, GenericRecord>> outputs = outputTopic.readKeyValuesToList();
        assertEquals(1, outputs.size());
        assertEquals(record1, outputs.getFirst().value);
    }

    @Test
    public void testProcessingOfUniqueRecords() {
        GenericRecord record1 = new GenericData.Record(schema);
        record1.put("name", "Alice");
        record1.put("age", 30);

        GenericRecord record2 = new GenericData.Record(schema);
        record2.put("name", "Alice");
        record2.put("age", 31); // Different age

        inputTopic.pipeInput(user1Key, record1);
        inputTopic.pipeInput(user1Key, record2);

        List<KeyValue<String, GenericRecord>> outputs = outputTopic.readKeyValuesToList();
        assertEquals(2, outputs.size());
        assertEquals(record1, outputs.get(0).value);
        assertEquals(record2, outputs.get(1).value);
    }

    @Test
    public void testFieldOrderVariation() {
        GenericRecord record1 = new GenericData.Record(schema);
        record1.put("name", "Alice");
        record1.put("age", 30);

        GenericRecord record2 = new GenericData.Record(schema);
        record2.put("age", 30);
        record2.put("name", "Alice"); // Different field order

        inputTopic.pipeInput(user1Key, record1);
        inputTopic.pipeInput(user1Key, record2);

        List<KeyValue<String, GenericRecord>> outputs = outputTopic.readKeyValuesToList();
        assertEquals(1, outputs.size());
        assertEquals(record1, outputs.getFirst().value);
    }

    @Test
    public void testNullValues() {
        inputTopic.pipeInput(user1Key, null);
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void testDeduplicationAcrossDifferentKeys() {

        GenericRecord record = new GenericData.Record(schema);
        record.put("name", "Alice");
        record.put("age", 30);

        inputTopic.pipeInput(user1Key, record);
        inputTopic.pipeInput(user2Key, record);

        List<KeyValue<String, GenericRecord>> outputs = outputTopic.readKeyValuesToList();
        assertEquals(2, outputs.size());
        assertEquals(user1Key, outputs.get(0).key);
        assertEquals(record, outputs.get(0).value);
        assertEquals(user2Key, outputs.get(1).key);
        assertEquals(record, outputs.get(1).value);
    }

    @Test
    public void testOscillatingValues() {
        GenericRecord record1 = new GenericData.Record(schema);
        record1.put("name", "Alice");
        record1.put("age", 30);

        GenericRecord record2 = new GenericData.Record(schema);
        record2.put("name", "Alice");
        record2.put("age", 31);

        inputTopic.pipeInput(user1Key, record1);
        inputTopic.pipeInput(user1Key, record2);
        inputTopic.pipeInput(user1Key, record1);
        inputTopic.pipeInput(user1Key, record2);

        // keep all the changes
        List<KeyValue<String, GenericRecord>> outputs = outputTopic.readKeyValuesToList();
        assertEquals(4, outputs.size());
        assertEquals(record1, outputs.get(0).value);
        assertEquals(record2, outputs.get(1).value);
        assertEquals(record1, outputs.get(2).value);
        assertEquals(record2, outputs.get(3).value);
    }

    @Test
    public void testLargeAvroRecords() {
        StringBuilder largeSchemaBuilder = new StringBuilder();
        largeSchemaBuilder.append("{\n\"type\": \"record\",\n\"name\": \"LargeRecord\",\n\"fields\": [\n");
        for (int i = 0; i < 1000; i++) {
            largeSchemaBuilder.append("{\"name\": \"field").append(i).append("\", \"type\": \"string\"},\n");
        }
        largeSchemaBuilder.append("{\"name\": \"field1000\", \"type\": \"string\"}\n]}");
        Schema largeSchema = new Schema.Parser().parse(largeSchemaBuilder.toString());

        GenericRecord largeRecord = new GenericData.Record(largeSchema);
        for (int i = 0; i <= 1000; i++) {
            largeRecord.put("field" + i, "value" + i);
        }

        inputTopic.pipeInput(user1Key, largeRecord);

        List<KeyValue<String, GenericRecord>> outputs = outputTopic.readKeyValuesToList();
        assertEquals(1, outputs.size());
        assertEquals(largeRecord, outputs.getFirst().value);
    }

    @Test
    public void testMultipleKeysAndValues() {

        GenericRecord record1 = new GenericData.Record(schema);
        record1.put("name", "Alice");
        record1.put("age", 30);

        GenericRecord record2 = new GenericData.Record(schema);
        record2.put("name", "Bob");
        record2.put("age", 25);

        GenericRecord record3 = new GenericData.Record(schema);
        record3.put("name", "Alice");
        record3.put("age", 31); // Age updated

        // Duplicate record for user2
        GenericRecord record4 = new GenericData.Record(schema);
        record4.put("name", "Bob");
        record4.put("age", 25);

            inputTopic.pipeInput(user1Key, record1);
            inputTopic.pipeInput(user2Key, record2);
            inputTopic.pipeInput(user1Key, record3);
            inputTopic.pipeInput(user2Key, record4);

        List<KeyValue<String, GenericRecord>> outputs = outputTopic.readKeyValuesToList();

        assertEquals(3, outputs.size());
    }

}
