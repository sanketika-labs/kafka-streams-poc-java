package com.sanketika.common.models;

public class ErrorConstants {

    public static final ErrorValue SCHEMA_VALIDATION_FAILED = new ErrorValue("ERR_1001", "Event failed the schema validation");
    public static final ErrorValue SCHEMA_FILE_MISSING = new ErrorValue("ERR_1002", "Event failed the schema validation due to schema file missing");
    public static final ErrorValue TRANSFORMATION_FAILED = new ErrorValue("ERR_1003", "Event failed the transformation");
    public static final ErrorValue PARTITION_BALANCER_FAILED = new ErrorValue("ERR_1004", "Event failed the partition balancer stage");

    private ErrorConstants() {
        // Prevent instantiation
    }
}
