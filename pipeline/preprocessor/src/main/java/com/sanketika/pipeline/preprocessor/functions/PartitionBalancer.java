package com.sanketika.pipeline.preprocessor.functions;

import com.sanketika.common.models.CanonicalEvent;
import com.sanketika.common.models.ErrorConstants;
import com.sanketika.common.models.ErrorEvent;
import com.sanketika.common.models.PipelineMeta;
import com.sanketika.common.models.ProcessingResult;
import com.sanketika.common.util.JsonUtil;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PartitionBalancer {
    private static final Logger logger = LoggerFactory.getLogger(PartitionBalancer.class);

    public static String mapKey(String key, ProcessingResult record) {
        if (record.getMetadata() == null) {
            return key;
        }
        return record.getMetadata().getOrDefault("operationType", key).toString();
    }

    @SuppressWarnings("unchecked")
    public static ProcessingResult mapValue(ProcessingResult record) {
        try {
            if (record.getMetadata() == null) {
                return record;
            }

            Object canonicalEventObj = record.getMetadata().getOrDefault("canonicalEvent", null);
            if (!(canonicalEventObj instanceof CanonicalEvent)) {
                return record;
            }

            CanonicalEvent canonicalEvent = (CanonicalEvent) canonicalEventObj;

            long preprocessorStart = Long.parseLong(
                record.getMetadata().getOrDefault("preprocessor_start", System.currentTimeMillis()).toString()
            );
            long preprocessorEnd = System.currentTimeMillis();
            long timeTaken = preprocessorEnd - preprocessorStart;

            PipelineMeta pipelineMeta = new PipelineMeta(preprocessorStart, timeTaken);
            canonicalEvent.setPipeline_meta(pipelineMeta);

            JsonUtil.Either<Throwable, String> result = JsonUtil.serialize(canonicalEvent);
            if (result.isRight()) {
                return new ProcessingResult(result.right(), true, null, record.getMetadata());
            } else {
                ErrorEvent errorEvent = new ErrorEvent(record.getPayload(), ErrorConstants.PARTITION_BALANCER_FAILED);
                return new ProcessingResult(record.getPayload(), false, errorEvent);
            }
        } catch (Exception e) {
            logger.error("Error in partition balancer: {}", e.getMessage(), e);
            ErrorEvent errorEvent = new ErrorEvent(record.getPayload(), ErrorConstants.PARTITION_BALANCER_FAILED);
            return new ProcessingResult(record.getPayload(), false, errorEvent);
        }
    }

    public static KeyValue<String, ProcessingResult> mapToNewKey(String key, ProcessingResult record) {
        String updatedKey = mapKey(key, record);
        ProcessingResult updatedValue = mapValue(record);
        return KeyValue.pair(updatedKey, updatedValue);
    }

    public static KStream<String, ProcessingResult> process(KStream<String, ProcessingResult> stream) {
        return stream
                .map(PartitionBalancer::mapToNewKey);
    }
}