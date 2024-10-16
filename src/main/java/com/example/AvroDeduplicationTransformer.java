package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

class AvroDeduplicationTransformer implements ValueTransformerWithKey<String, GenericRecord, GenericRecord> {

    Logger logger = Logger.getLogger(AvroDeduplicationTransformer.class);

    private final String storeName;
    private KeyValueStore<String, String> kvStore;
    private MessageDigest digest;
    private ObjectMapper objectMapper;

    AvroDeduplicationTransformer(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.kvStore = context.getStateStore(storeName);
        this.objectMapper = new ObjectMapper();
        try {
            this.digest = MessageDigest.getInstance("SHA-256");
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize MessageDigest", e);
        }
    }

    @Override
    public GenericRecord transform(String key, GenericRecord value) {
        try {
            if (value == null) {
                return null;
            }

            Object canonicalizedRecord = AvroCanonicalizer.canonicalize(value);

            String canonicalJson = objectMapper.writeValueAsString(canonicalizedRecord);
            logger.info(canonicalJson);

            byte[] hashBytes = digest.digest(canonicalJson.getBytes(StandardCharsets.UTF_8));
            String newHash = bytesToHex(hashBytes);
            logger.info(newHash);

            String oldHash = kvStore.get(key);

            if (newHash.equals(oldHash)) {
                return null;
            } else {
                kvStore.put(key, newHash);
                return value;
            }
        } catch (Exception e) {
            // TODO - improve exception handling
            e.printStackTrace();
            return null;
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
