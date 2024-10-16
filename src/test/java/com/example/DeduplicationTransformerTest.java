package com.example;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DeduplicationTransformerTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;

    private final Serde<String> stringSerde = Serdes.String();

    Topology topo = DedupTopologyProducer.createTopology();

    @Test
    public void testDeduplicationOfDuplicates() {
        String key = "user1";
        String value = "{\"name\":\"Alice\",\"age\":30}";

        inputTopic.pipeInput(key, value);
        inputTopic.pipeInput(key, value);

        List<KeyValue<String, String>> outputs = outputTopic.readKeyValuesToList();
        assertEquals(1, outputs.size());
        assertEquals(key, outputs.getFirst().key);
        assertEquals(value, outputs.getFirst().value);
    }

    @Test
    public void testProcessingOfUniqueRecords() {
        String key = "user1";
        String value1 = "{\"name\":\"Alice\",\"age\":30}";
        String value2 = "{\"name\":\"Alice\",\"age\":31}";

        inputTopic.pipeInput(key, value1);
        inputTopic.pipeInput(key, value2);

        List<KeyValue<String, String>> outputs = outputTopic.readKeyValuesToList();
        assertEquals(2, outputs.size());
        assertEquals(value1, outputs.get(0).value);
        assertEquals(value2, outputs.get(1).value);
    }

    @Test
    public void testDeduplicationAcrossDifferentKeys() {
        String key1 = "user1";
        String key2 = "user2";
        String value = "{\"name\":\"Alice\",\"age\":30}";

        inputTopic.pipeInput(key1, value);
        inputTopic.pipeInput(key2, value);

        List<KeyValue<String, String>> outputs = outputTopic.readKeyValuesToList();
        assertEquals(2, outputs.size());
        assertEquals(key1, outputs.get(0).key);
        assertEquals(value, outputs.get(0).value);
        assertEquals(key2, outputs.get(1).key);
        assertEquals(value, outputs.get(1).value);
    }


    @Test
    public void testJsonNormalization() {
        String key = "user1";
        String value1 = "{\"name\":\"Alice\",\"age\":30}";
        String value2 = "{\"age\":30,\"name\":\"Alice\"}";

        inputTopic.pipeInput(key, value1);
        inputTopic.pipeInput(key, value2);

        List<KeyValue<String, String>> outputs = outputTopic.readKeyValuesToList();
        outputs.forEach(System.out::println);
        assertEquals(1, outputs.size());
        assertEquals(value1, outputs.getFirst().value);
    }

    @Test
    public void testNullValues() {
        String key = "user1";
        String value = null;

        inputTopic.pipeInput(key, value);

        assertTrue(outputTopic.isEmpty());
    }


    @Test
    public void testMalformedJson() {
        String key = "user1";
        String validValue = "{\"name\":\"Alice\"}";
        String invalidValue = "{\"name\":\"Alice\"";
        String validValue2 = "{\"name\":\"Bob\"}";

        inputTopic.pipeInput(key, validValue);

        inputTopic.pipeInput(key, invalidValue);

        inputTopic.pipeInput(key, validValue2);

        List<KeyValue<String, String>> outputs = outputTopic.readKeyValuesToList();
        assertEquals(2, outputs.size());
        assertEquals(validValue, outputs.get(0).value);
        assertEquals(validValue2, outputs.get(1).value);
    }

    @Test
    public void testOscillatingValues() {
        String key = "user1";
        String valueA = "{\"status\":\"active\"}";
        String valueB = "{\"status\":\"inactive\"}";

        inputTopic.pipeInput(key, valueA);
        inputTopic.pipeInput(key, valueB);
        inputTopic.pipeInput(key, valueA);
        inputTopic.pipeInput(key, valueB);

        // keeping all the changes
        List<KeyValue<String, String>> outputs = outputTopic.readKeyValuesToList();
        assertEquals(4, outputs.size());
        assertEquals(valueA, outputs.get(0).value);
        assertEquals(valueB, outputs.get(1).value);
        assertEquals(valueA, outputs.get(2).value);
        assertEquals(valueB, outputs.get(3).value);
    }

    @Test
    public void testLargeJsonObjects() {
        String key = "user1";
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (int i = 0; i < 1000; i++) {
            sb.append("\"key").append(i).append("\":").append(i).append(",");
        }
        sb.append("\"key1000\":1000}");
        String largeJson = sb.toString();

        inputTopic.pipeInput(key, largeJson);

        List<KeyValue<String, String>> outputs = outputTopic.readKeyValuesToList();
        assertEquals(1, outputs.size());
        assertEquals(largeJson, outputs.get(0).value);
    }

    @Test
    public void testMultipleKeysAndValues() {
        Map<String, String> records = new HashMap<>();
        records.put("user1", "{\"name\":\"Alice\",\"age\":30}");
        records.put("user2", "{\"name\":\"Bob\",\"age\":25}");
        records.put("user3", "{\"name\":\"Charlie\",\"age\":35}");
        records.put("user1", "{\"name\":\"Alice\",\"age\":31}"); // upd user1
        records.put("user2", "{\"name\":\"Bob\",\"age\":25}"); // dupe user2

        for (Map.Entry<String, String> entry : records.entrySet()) {
            inputTopic.pipeInput(entry.getKey(), entry.getValue());
        }

        List<KeyValue<String, String>> outputs = outputTopic.readKeyValuesToList();
        assertEquals(3, outputs.size());
    }


    @BeforeEach
    public void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ks-dedup");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        testDriver = new TopologyTestDriver(topo, props);

        String INPUT_TOPIC = "input-topic";
        inputTopic = testDriver.createInputTopic(INPUT_TOPIC, stringSerde.serializer(), stringSerde.serializer());
        String OUTPUT_TOPIC = "output-topic";
        outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringSerde.deserializer(), stringSerde.deserializer());
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

}
