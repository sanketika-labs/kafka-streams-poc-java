package com.sanketika.functions;

import com.sanketika.models.ProcessingResult;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Map;

public class PartitionBalancer {

    public static KeyValue<String, ProcessingResult> mapToNewKey(String key, ProcessingResult record) {
        String newKey = key;

        if (record.getMetadata() != null) {
            Map<String, Object> metadata = record.getMetadata();
            Object operationType = metadata.getOrDefault("operationType", key);
            if (operationType != null) {
                newKey = operationType.toString();
            }
        }

        return KeyValue.pair(newKey, record);
    }

    public static KStream<String, ProcessingResult> process(KStream<String, ProcessingResult> stream) {
        return stream
                .map(PartitionBalancer::mapToNewKey);
    }
}
