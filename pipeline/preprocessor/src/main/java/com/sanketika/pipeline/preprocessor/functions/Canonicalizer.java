package com.sanketika.pipeline.preprocessor.functions;

import com.sanketika.common.models.CanonicalEvent;
import com.sanketika.common.models.ErrorConstants;
import com.sanketika.common.models.ErrorEvent;
import com.sanketika.common.models.ProcessingResult;
import com.sanketika.common.util.CommonUtil;
import com.sanketika.common.util.JsonUtil;
import org.apache.kafka.streams.kstream.KStream;

import java.util.HashMap;
import java.util.Map;

public class Canonicalizer {

    private static Map.Entry<Map<String, Object>, Map<String, Object>> splitCdcMetaAndPayload(Map<String, Object> event, String operationType) {
        Map<String, Object> input = event;

        if ("a".equalsIgnoreCase(operationType)) {
            Object updateObj = event.getOrDefault("update", new HashMap<>());
            if (updateObj instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> updateMap = (Map<String, Object>) updateObj;
                input = updateMap;
            }
        }

        Map<String, Object> cdcMeta = new HashMap<>();
        Map<String, Object> payload = new HashMap<>();

        for (Map.Entry<String, Object> entry : input.entrySet()) {
            if (entry.getKey().startsWith("dm_")) {
                cdcMeta.put(entry.getKey(), entry.getValue());
            } else {
                payload.put(entry.getKey(), entry.getValue());
            }
        }

        return Map.entry(cdcMeta, payload);
    }

    private static ProcessingResult transform(ProcessingResult record) {
        try {
            Map<String, Object> event = JsonUtil.deserializeJson(record.getPayload());
            String operationType = CommonUtil.getOperationType(event);
            Map.Entry<Map<String, Object>, Map<String, Object>> processedEvent = splitCdcMetaAndPayload(event, operationType);

            Map<String, Object> cdcMeta = processedEvent.getKey();
            Map<String, Object> payload = processedEvent.getValue();

            String tableName = CommonUtil.getTableName(cdcMeta);

            CanonicalEvent canonicalEvent = new CanonicalEvent(tableName, cdcMeta, payload);

            Map<String, Object> metadata = new HashMap<>(record.getMetadata() != null ? record.getMetadata() : new HashMap<>());
            metadata.put("operationType", operationType);
            metadata.put("canonicalEvent", canonicalEvent);

            return new ProcessingResult(record.getPayload(), true, null, metadata);
        } catch (Exception e) {
            e.printStackTrace();
            ErrorEvent failedEvent = new ErrorEvent(record.getPayload(), ErrorConstants.TRANSFORMATION_FAILED);
            return new ProcessingResult(record.getPayload(), false, failedEvent);
        }
    }

    public static KStream<String, ProcessingResult>[] process(KStream<String, ProcessingResult> stream) {
        return stream
                .filter((key, value) -> value.isSuccess())
                .mapValues(Canonicalizer::transform)
                .branch(
                        (key, value) -> value.isSuccess(),
                        (key, value) -> !value.isSuccess()
                );
    }
}