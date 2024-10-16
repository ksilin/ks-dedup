package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.erdtman.jcs.JsonCanonicalizer;
import org.jboss.logging.Logger;

import java.security.MessageDigest;

class JsonDeduplicationTransformer implements ValueTransformerWithKey<String, String, String> {

    Logger logger = Logger.getLogger(JsonDeduplicationTransformer.class);

    private final String storeName;
    private KeyValueStore<String, String> kvStore;
    private ObjectMapper objectMapper;
    private MessageDigest digest;

    JsonDeduplicationTransformer(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.kvStore = context.getStateStore(storeName);
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        try {
            this.digest = MessageDigest.getInstance("SHA-256");
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize MessageDigest", e);
        }
    }

    @Override
    public String transform(String key, String value) {
        try {
            JsonCanonicalizer jc = new JsonCanonicalizer(value);
            String canonicalJson = jc.getEncodedString();
            logger.info(canonicalJson);
            byte[] hashBytes = digest.digest(canonicalJson.getBytes("UTF-8"));
            String newHash = bytesToHex(hashBytes);

            if (newHash.equals(kvStore.get(key))) {
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
