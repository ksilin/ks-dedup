package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.erdtman.jcs.JsonCanonicalizer;
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

            // Canonicalize the Avro record
            Object canonicalizedRecord = AvroCanonicalizer.canonicalize(value);

            // Serialize the canonicalized object to JSON string
            String canonicalJson = objectMapper.writeValueAsString(canonicalizedRecord);
            logger.info("canonicalJson");
            logger.info(canonicalJson);

            // Compute the hash
            byte[] hashBytes = digest.digest(canonicalJson.getBytes(StandardCharsets.UTF_8));
            String newHash = bytesToHex(hashBytes);
            logger.info("newHash");
            logger.info(newHash);

            // Retrieve the previous hash
            String oldHash = kvStore.get(key);

            if (newHash.equals(oldHash)) {
                // Duplicate detected; do not forward
                return null;
            } else {
                // Update the hash and forward the record
                kvStore.put(key, newHash);
                return value;
            }
        } catch (Exception e) {
            // Log the exception and skip the record
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
