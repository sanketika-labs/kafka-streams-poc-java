package com.sanketika.common.models;

public class ErrorConstants {

    public static final ErrorValue SCHEMA_VALIDATION_FAILED = new ErrorValue("SCHEMA_VALIDATION_FAILED", "Input event failed schema validation");
    public static final ErrorValue SCHEMA_FILE_MISSING = new ErrorValue("SCHEMA_FILE_MISSING", "Schema file not found");
    public static final ErrorValue TRANSFORMATION_FAILED = new ErrorValue("TRANSFORMATION_FAILED", "Failed to transform event");
    public static final ErrorValue PARTITION_BALANCER_FAILED = new ErrorValue("PARTITION_BALANCER_FAILED", "Failed to balance partitions");

    private ErrorConstants() {
        // Prevent instantiation
    }
}
