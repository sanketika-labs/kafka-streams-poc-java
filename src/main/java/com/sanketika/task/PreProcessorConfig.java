package com.sanketika.task;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Centralized configuration for the Kafka Streams pipeline
 */
public class PreProcessorConfig {
    private static final Config config = loadConfig();

    // Kafka configuration
    public static final String kafkaApplicationId = getStringOrElse("kafka.application.id", "kafka-streams-app-default");
    public static final String kafkaBootstrapServers = getStringOrElse("kafka.bootstrap.servers", "localhost:9092");
    public static final int kafkaNumThreads = getIntOrElse("kafka.numThreads", 1);

    // Topic configuration
    public static final String inputTopic = getStringOrElse("kafka.topics.input", "input-default");
    public static final String outputTopic = getStringOrElse("kafka.topics.output", "output-default");
    public static final String failedTopic = getStringOrElse("kafka.topics.failed", "failed-default");

    // Schema configuration
    public static final Set<String> schemaKeys = getSchemaKeys();

    private static Set<String> getSchemaKeys() {
        if (config.hasPath("schemas")) {
            return StreamSupport.stream(config.getConfig("schemas").root().keySet().spliterator(), false)
                    .collect(Collectors.toSet());
        } else {
            return Collections.emptySet();
        }
    }

    public static Map<String, String> getAllSchemaPaths() {
        if (schemaKeys.isEmpty()) {
            System.err.println("Warning: No schemas configured");
            return Collections.emptyMap();
        } else {
            Map<String, String> result = new HashMap<>();
            for (String key : schemaKeys) {
                result.put(key, config.getString("schemas." + key));
            }
            return result;
        }
    }

    private static String getStringOrElse(String path, String defaultValue) {
        try {
            if (config.hasPath(path)) {
                return config.getString(path);
            }
        } catch (ConfigException e) {
            // Fall back to default
        }
        return defaultValue;
    }

    private static int getIntOrElse(String path, int defaultValue) {
        try {
            if (config.hasPath(path)) {
                return config.getInt(path);
            }
        } catch (ConfigException e) {
            // Fall back to default
        }
        return defaultValue;
    }

    private static Config loadConfig() {
        try {
            return ConfigFactory.load();
        } catch (Exception e) {
            System.err.println("Fatal error: Failed to load application.conf: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Application cannot start without valid configuration", e);
        }
    }
}
