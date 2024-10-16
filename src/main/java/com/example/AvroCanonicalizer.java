package com.example;
import java.util.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class AvroCanonicalizer {

    public static Object canonicalize(Object datum) {
        if (datum == null) {
            return null;
        } else if (datum instanceof GenericRecord) {
            GenericRecord record = (GenericRecord) datum;
            Map<String, Object> sortedMap = new TreeMap<>();
            for (Schema.Field field : record.getSchema().getFields()) {
                String fieldName = field.name();
                Object value = record.get(fieldName);
                sortedMap.put(fieldName, canonicalize(value));
            }
            return sortedMap;
        } else if (datum instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) datum;
            Map<Object, Object> sortedMap = new TreeMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                sortedMap.put(entry.getKey(), canonicalize(entry.getValue()));
            }
            return sortedMap;
        } else if (datum instanceof Collection) {
            Collection<?> collection = (Collection<?>) datum;
            List<Object> canonicalList = new ArrayList<>();
            for (Object item : collection) {
                canonicalList.add(canonicalize(item));
            }
            return canonicalList;
        } else {
            // Primitive types and others
            return datum;
        }
    }
}
