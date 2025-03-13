package com.sanketika.functions;

import com.sanketika.models.*;
import com.sanketika.util.CommonUtil;
import com.sanketika.util.JsonUtil;
import org.apache.kafka.streams.kstream.KStream;

import java.util.HashMap;
import java.util.Map;

public class Canonicalizer {

    private static Map.Entry<Map<String, Object>, Map<String, Object>> splitCdcMetaAndPayload(Map<String, Object> event, String operationType) {
        Map<String, Object> input = event;

        if ("a".equalsIgnoreCase(operationType)) {
            Object updateObj = event.get("update");
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

    private static ProcessingResult transform(String record) {
        try {
            Map<String, Object> event = JsonUtil.deserializeJson(record);
            String operationType = CommonUtil.getOperationType(event);
            Map.Entry<Map<String, Object>, Map<String, Object>> processedEvent = splitCdcMetaAndPayload(event, operationType);

            Map<String, Object> cdcMeta = processedEvent.getKey();
            Map<String, Object> payload = processedEvent.getValue();

            String tableName = CommonUtil.getTableName(cdcMeta);

            PipelineMeta pipelineMeta = new PipelineMeta(System.currentTimeMillis(), 200);
            CanonicalEvent canonicalEvent = new CanonicalEvent(tableName, cdcMeta, payload, pipelineMeta);

            JsonUtil.Either<Throwable, String> result = JsonUtil.serialize(canonicalEvent);
            if (result.isRight()) {
                // Create metadata for the processing result
                Map<String, Object> metadata = new HashMap<>();
                metadata.put("operationType", operationType);
                metadata.put("tableName", tableName);

                return new ProcessingResult(result.right(), true, null, metadata);
            } else {
                throw (Exception) result.left();
            }
        } catch (Exception e) {
            e.printStackTrace();
            ErrorEvent failedEvent = new ErrorEvent(record, ErrorConstants.TRANSFORMATION_FAILED);
            return new ProcessingResult(record, false, failedEvent);
        }
    }

    public static KStream<String, ProcessingResult>[] process(KStream<String, ProcessingResult> stream) {
        return stream
                .filter((key, value) -> value.isSuccess())
                .mapValues(result -> transform(result.getPayload()))
                .branch(
                        (key, value) -> value.isSuccess(),
                        (key, value) -> !value.isSuccess()
                );
    }
}
