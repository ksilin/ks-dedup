package com.example;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public class AvroDeduplicationDLQProcessor implements FixedKeyProcessor<String, GenericRecord, GenericRecord> {

    private static final Logger logger = Logger.getLogger(AvroDeduplicationDLQProcessor.class);

    private final String storeName;
    private KeyValueStore<String, String> kvStore;
    private MessageDigest digest;
    private ObjectMapper objectMapper;
    private FixedKeyProcessorContext<String, GenericRecord> context;
    private final String successChildName;
    private final String dlqChildName;

    AvroDeduplicationDLQProcessor(String storeName, String successChildName, String dlqChildName) {
        this.storeName = storeName;
        this.successChildName = successChildName;
        this.dlqChildName = dlqChildName;
    }

    @Override
    public void init(FixedKeyProcessorContext<String, GenericRecord> context) {
        this.context = context;
        this.kvStore = context.getStateStore(storeName);
        this.objectMapper = new ObjectMapper();
        try {
            this.digest = MessageDigest.getInstance("SHA-256");
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize MessageDigest", e);
        }
    }

    @Override
    public void process(FixedKeyRecord<String, GenericRecord> record) {
        try {
            if (record.value() == null) {
                logger.warn("Received record with null value for key: " + record.key());
                // for sake of demo, we are considering tombstones to be failures
                context.forward(record, dlqChildName);
                return;
            }

            Object canonicalizedRecord = AvroCanonicalizer.canonicalize(record.value());
            String canonicalJson = objectMapper.writeValueAsString(canonicalizedRecord);
            logger.debug("Canonicalized JSON of the record: " + canonicalJson);

            byte[] hashBytes = digest.digest(canonicalJson.getBytes(StandardCharsets.UTF_8));
            String newHash = bytesToHex(hashBytes);
            logger.debug("New hash: " + newHash);

            if (!newHash.equals(kvStore.get(record.key()))) {
                kvStore.put(record.key(), newHash);
                context.forward(record, successChildName);
                logger.debug("Forwarded new or updated record with key: " + record.key());
            } else {
                logger.debug("Skipped duplicate record with key: " + record.key());
            }
        } catch (Exception e) {
            logger.error("Error processing record with key: " + record.key(), e);
            context.forward(record, dlqChildName);
        }
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes)
            sb.append(String.format("%02x", b));
        return sb.toString();
    }

    @Override
    public void close() {
    }
}
