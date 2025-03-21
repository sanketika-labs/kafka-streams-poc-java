package com.sanketika.pipeline.preprocessor.functions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import com.sanketika.common.models.ErrorConstants;
import com.sanketika.common.models.ErrorEvent;
import com.sanketika.common.models.ProcessingResult;
import com.sanketika.common.util.CommonUtil;
import com.sanketika.common.util.JsonUtil;
import com.sanketika.pipeline.preprocessor.task.PreProcessorConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Validator {
    private static final Logger logger = LoggerFactory.getLogger(Validator.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
    private static final ConcurrentHashMap<String, JsonSchema> schemaMap = new ConcurrentHashMap<>();

    public static Map<String, JsonSchema> loadJsonSchemas() {
        if (!schemaMap.isEmpty()) {
            return schemaMap;
        }

        Map<String, String> schemaPaths = PreProcessorConfig.getAllSchemaPaths();

        for (Map.Entry<String, String> entry : schemaPaths.entrySet()) {
            String schemaName = entry.getKey();
            String schemaPath = entry.getValue();

            JsonSchema schema = loadSchema(schemaName, schemaPath);
            if (schema != null) {
                schemaMap.put(schemaName, schema);
            }
        }

        if (schemaMap.isEmpty()) {
            logger.warn("No schemas were loaded successfully. Validation will fail.");
        } else {
            logger.info("Successfully loaded {} JSON schemas: {}", schemaMap.size(),
                      String.join(", ", schemaMap.keySet()));
        }

        return schemaMap;
    }

    private static JsonSchema loadSchema(String schemaName, String schemaPath) {
        try {
            InputStream schemaStream = Validator.class.getResourceAsStream(schemaPath);

            if (schemaStream == null) {
                logger.warn("Schema file not found at {} for schema '{}'", schemaPath, schemaName);
                return null;
            }

            try (InputStreamReader reader = new InputStreamReader(schemaStream, StandardCharsets.UTF_8)) {
                JsonNode schemaNode = objectMapper.readTree(reader);
                JsonSchema jsonSchema = schemaFactory.getSchema(schemaNode);
                logger.info("Successfully loaded schema '{}' from {}", schemaName, schemaPath);
                return jsonSchema;
            }
        } catch (Exception exception) {
            logger.error("Error loading schema '{}' from {}: {}", schemaName, schemaPath, exception.getMessage());
            return null;
        }
    }

    public static KStream<String, ProcessingResult>[] process(KStream<String, String> stream) {
        return stream.mapValues(Validator::validate)
                .peek((key, result) -> {
                    if (result.isSuccess()) {
                        logger.debug("[VALIDATOR] Message passed validation");
                    } else {
                        logger.debug("[VALIDATOR] Message failed validation: {}",
                                 result.getError().getError().getMsg());
                    }
                })
                .branch(
                        (key, value) -> value.isSuccess(),
                        (key, value) -> !value.isSuccess()
                );
    }

    private static String getSchemaKeyName(String record) {
        try {
            Map<String, Object> event = JsonUtil.deserializeJson(record);
            String operationType = CommonUtil.getOperationType(event);
            return operationType.toLowerCase();
        } catch (Exception e) {
            logger.error("Error determining schema type: {}", e.getMessage(), e);
            return "";
        }
    }

    private static ProcessingResult validate(String record) {
        if (schemaMap.isEmpty()) {
            logger.info("[VALIDATOR] No schemas loaded, loading schemas now...");
            loadJsonSchemas();
        }

        String schemaName = getSchemaKeyName(record);
        logger.debug("[VALIDATOR] Using schema: {}", schemaName);
        JsonSchema schema = schemaMap.get(schemaName);

        if (schema != null) {
            if (validateWithSchema(record, schema, schemaName)) {
                Map<String, Object> metadata = new HashMap<>();
                metadata.put("preprocessor_start", System.currentTimeMillis());
                return new ProcessingResult(record, true, null, metadata);
            } else {
                ErrorEvent errorEvent = new ErrorEvent(record, ErrorConstants.SCHEMA_VALIDATION_FAILED);
                return new ProcessingResult(record, false, errorEvent);
            }
        } else {
            logger.error("[VALIDATOR] Schema not found: {}", schemaName);
            ErrorEvent errorEvent = new ErrorEvent(record, ErrorConstants.SCHEMA_FILE_MISSING);
            return new ProcessingResult(record, false, errorEvent);
        }
    }

    private static boolean validateWithSchema(String json, JsonSchema schema, String schemaName) {
        try {
            JsonNode jsonNode = objectMapper.readTree(json);
            Set<ValidationMessage> validationMessages = schema.validate(jsonNode);

            if (!validationMessages.isEmpty()) {
                logger.warn("Validation failed for schema '{}' with {} errors:", schemaName, validationMessages.size());
                for (ValidationMessage msg : validationMessages) {
                    logger.warn(" - {}", msg.getMessage());
                }
            }

            return validationMessages.isEmpty();
        } catch (Exception e) {
            logger.error("Error during validation against schema '{}': {}", schemaName, e.getMessage());
            return false;
        }
    }
}