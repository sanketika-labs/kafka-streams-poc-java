package com.sanketika.pipeline.preprocessor.task;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Centralized configuration for the Kafka Streams pipeline
 */
public class PreProcessorConfig {
    private static final Logger logger = LoggerFactory.getLogger(PreProcessorConfig.class);
    private static final Config config = loadConfig();

    // Kafka configuration
    public static final String kafkaApplicationId = getStringOrElse("kafka.application.id", "kafka-streams-app-default");
    public static final String kafkaBootstrapServers = getStringOrElse("kafka.bootstrap.servers", "localhost:9092");
    public static final int kafkaNumThreads = getIntOrElse("kafka.numThreads", 1);

    // Topic configuration
    public static final List<String> inputTopics = getStringListOrElse("kafka.topics.inputs", Collections.singletonList("input-default"));
    public static final String outputSuffix = getStringOrElse("kafka.topics.output_suffix", "_canonical");
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
            logger.warn("No schemas configured");
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

    private static List<String> getStringListOrElse(String path, List<String> defaultValue) {
        try {
            if (config.hasPath(path)) {
                return config.getStringList(path);
            }
        } catch (ConfigException e) {
            // Fall back to default
            logger.error("Error reading list from config: {}", e.getMessage());
        }
        return defaultValue;
    }

    /**
     * Gets the output topic name for a given input topic by appending the configured suffix
     * @param inputTopic the input topic name
     * @return the corresponding output topic name
     */
    public static String getOutputTopicForInput(String inputTopic) {
        return inputTopic + outputSuffix;
    }

    private static Config loadConfig() {
        try {
            // Try to load from external file first
            File externalConfig = new File("/data/conf/preprocessor.conf");
            if (externalConfig.exists() && externalConfig.canRead()) {
                logger.info("Loading configuration from external file: {}", externalConfig.getAbsolutePath());
                return ConfigFactory.parseFile(externalConfig).resolve();
            }

            // Then try classpath
            logger.info("Loading configuration from classpath: preprocessor.conf");
            Config classpathConfig = ConfigFactory.parseResources("preprocessor.conf");
            if (!classpathConfig.isEmpty()) {
                return classpathConfig.resolve();
            }

            // Finally, fall back to application.conf (backward compatibility)
            logger.info("Falling back to application.conf in classpath");
            return ConfigFactory.load();
        } catch (Exception e) {
            logger.error("Fatal error: Failed to load configuration: {}", e.getMessage(), e);
            throw new RuntimeException("Application cannot start without valid configuration", e);
        }
    }
}