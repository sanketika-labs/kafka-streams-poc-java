package com.sanketika.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.BiConsumer;

public class CommonUtil {
    private static final Logger logger = LoggerFactory.getLogger(CommonUtil.class);

    private CommonUtil() {
        // Prevent instantiation
    }

    @SuppressWarnings("unchecked")
    public static String getOperationType(Map<String, Object> event) {
        if (event.containsKey("dm_operation_type")) {
            return event.get("dm_operation_type").toString();
        } else if (event.containsKey("update")) {
            Object updateObj = event.get("update");
            if (updateObj instanceof Map) {
                Map<String, Object> update = (Map<String, Object>) updateObj;
                if (update.containsKey("dm_operation_type")) {
                    return update.get("dm_operation_type").toString();
                }
            }
        }
        return "";
    }

    public static String getTableName(Map<String, Object> event) {
        Object tableName = event.getOrDefault("dm_table", "");
        return tableName != null ? tableName.toString() : "";
    }

    public static BiConsumer<String, String> peek(String result) {
        return (key, value) -> {
            logger.debug("[{} Branch] Key: {}, Value: {}", result, key, value);
        };
    }
}
