package com.sanketika.pipeline.preprocessor.task;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanketika.common.models.CanonicalEvent;
import com.sanketika.common.models.ErrorEvent;
import com.sanketika.common.models.ProcessingResult;
import com.sanketika.common.util.JsonUtil;
import com.sanketika.common.util.LogbackConfigurer;
import com.sanketika.pipeline.preprocessor.functions.Canonicalizer;
import com.sanketika.pipeline.preprocessor.functions.PartitionBalancer;
import com.sanketika.pipeline.preprocessor.functions.Validator;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Map;

public class PreprocessorTask {
    private static final Logger logger = LoggerFactory.getLogger(PreprocessorTask.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        // First make sure we see something with System.out
        System.out.println("Starting PreprocessorTask...");

        // Configure Logback
        LogbackConfigurer.configureLogback();

        try {
            Properties props = createKafkaProperties();
            StreamsBuilder builder = new StreamsBuilder();

            buildStreamsTopology(builder);

            final KafkaStreams streams = new KafkaStreams(builder.build(), props);

            // Clean up state on shutdown
            streams.setStateListener((newState, oldState) -> {
                logger.info("Kafka Streams state changed from {} to {}", oldState, newState);
            });

            // Handle uncaught exceptions
            streams.setUncaughtExceptionHandler((thread, throwable) -> {
                logger.error("Uncaught exception in Kafka Streams thread {}: {}", thread, throwable.getMessage(), throwable);
            });

            // Setup shutdown hook
            setupShutdownHook(streams);

            try {
                streams.cleanUp(); // Clean local state stores before starting
                streams.start();
                logger.info("Kafka Streams application started, listening for messages on topics: {}",
                    String.join(", ", PreProcessorConfig.inputTopics));
            } catch (Exception e) {
                logger.error("Error during streams execution: {}", e.getMessage(), e);
                streams.close(Duration.ofMinutes(1));
            }

        } catch (Exception e) {
            logger.error("Failed to start Kafka Streams application: {}", e.getMessage(), e);
            System.exit(1);
        }
    }

    private static Properties createKafkaProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, PreProcessorConfig.kafkaApplicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, PreProcessorConfig.kafkaBootstrapServers);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, PreProcessorConfig.kafkaNumThreads);

        // Add consumer configuration for better debugging
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

        // Add default serializers/deserializers
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Add configuration for commit interval (defaults to 30 seconds)
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000); // Commit every 1 second

        // For debugging
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0); // Disable caching

        logger.debug("Kafka configuration: {}", props);

        return props;
    }

    private static void buildStreamsTopology(StreamsBuilder builder) {
        List<String> inputTopics = PreProcessorConfig.inputTopics;
        List<KStream<String, ProcessingResult>> allFailedStreams = new ArrayList<>();

        // Log topology for debugging
        logger.info("Building topology for {} input topics", inputTopics.size());
        logger.info("Input topics: {}", String.join(", ", inputTopics));
        logger.info("Output suffix: {}", PreProcessorConfig.outputSuffix);
        logger.info("Failed topic: {}", PreProcessorConfig.failedTopic);

        // Process each input topic
        for (String inputTopic : inputTopics) {
            String defaultOutputTopic = PreProcessorConfig.getOutputTopicForInput(inputTopic);

            logger.info("Setting up processing pipeline: {} -> {} (default)", inputTopic, defaultOutputTopic);

            // Explicitly specify serdes for input topic
            KStream<String, String> sourceStream = builder.stream(
                inputTopic,
                Consumed.with(Serdes.String(), Serdes.String())
            );

            KStream<String, ProcessingResult>[] validatorBranchedStreams = Validator.process(sourceStream);
            KStream<String, ProcessingResult> validatorSuccessStream = validatorBranchedStreams[0];
            KStream<String, ProcessingResult> validatorFailureStream = validatorBranchedStreams[1];

            KStream<String, ProcessingResult>[] canonicalizerBranchedStreams = Canonicalizer.process(validatorSuccessStream);
            KStream<String, ProcessingResult> canonicalizerSuccessStream = canonicalizerBranchedStreams[0];
            KStream<String, ProcessingResult> canonicalizerFailureStream = canonicalizerBranchedStreams[1];

            KStream<String, ProcessingResult> partitionBalancerBranchedStream = PartitionBalancer.process(canonicalizerSuccessStream);

            // Sink to topic-specific output
            // Now we pass the inputTopic instead of directly computing the output topic
            sinkEvents(partitionBalancerBranchedStream, inputTopic);

            // Collect all failure streams to process together
            allFailedStreams.add(validatorFailureStream);
            allFailedStreams.add(canonicalizerFailureStream);
        }

        // Process all failed streams to the common failed topic
        processFailedStreams(allFailedStreams.toArray(new KStream[0]));
    }

    private static void sinkEvents(KStream<String, ProcessingResult> stream, String inputTopic) {
        // First branch by whether we have a canonicalEvent with table name in metadata
        KStream<String, ProcessingResult>[] branchedStreams = stream.branch(
            // First branch: Records with canonicalEvent in metadata that has a table name
            (key, value) -> {
                if (value.getMetadata() != null && value.getMetadata().containsKey("canonicalEvent")) {
                    Object canonicalEventObj = value.getMetadata().get("canonicalEvent");
                    if (canonicalEventObj instanceof CanonicalEvent) {
                        CanonicalEvent event = (CanonicalEvent) canonicalEventObj;
                        return event.getTableName() != null && !event.getTableName().isEmpty();
                    }
                }
                return false;
            },
            // Second branch: Records without table name
            (key, value) -> true  // All remaining records
        );

        // Process records with table name - use the table name for output topic
        branchedStreams[0]
            .selectKey((key, value) -> {
                // Extract the canonicalEvent from metadata to use in the topic selector
                CanonicalEvent event = (CanonicalEvent) value.getMetadata().get("canonicalEvent");
                return event.getTableName(); // Store table name as key for use by the topic selector
            })
            .mapValues(ProcessingResult::getPayload)
            .to((key, value, recordContext) -> {
                try {
                    // Use the key (which now contains the table name) to determine the output topic
                    String tableName = key;
                    String outputTopic = tableName + PreProcessorConfig.outputSuffix;
                    logger.debug("[Table-Based Routing] Routing to topic: {}", outputTopic);
                    return outputTopic;
                } catch (Exception e) {
                    logger.error("Error determining output topic, falling back to default: {}", e.getMessage());
                    return PreProcessorConfig.getOutputTopicForInput(inputTopic);
                }
            }, Produced.with(Serdes.String(), Serdes.String()));

        // Process records without table name - use the default inputTopic+suffix
        String defaultOutputTopic = PreProcessorConfig.getOutputTopicForInput(inputTopic);
        branchedStreams[1]
            .mapValues(ProcessingResult::getPayload)
            .peek((key, value) -> logger.debug("[Default Routing] Key: {}, Value: {}, Topic: {}", key, value, defaultOutputTopic))
            .to(defaultOutputTopic, Produced.with(Serdes.String(), Serdes.String()));
    }

    private static void processFailedStreams(KStream<String, ProcessingResult>[] streams) {
        String topic = PreProcessorConfig.failedTopic;

        for (KStream<String, ProcessingResult> stream : streams) {
            stream
                    .filter((key, value) -> !value.isSuccess())
                    .mapValues(result -> {
                        JsonUtil.Either<Throwable, String> serializedError = JsonUtil.serialize(result.getError());
                        return serializedError.getOrElse("");
                    })
                    .peek((key, value) -> logger.debug("[Failed Branch] Key: {}, Value: {}", key, value))
                    .to(topic, Produced.with(Serdes.String(), Serdes.String()));
        }
    }

    private static void setupShutdownHook(KafkaStreams streams) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook triggered - gracefully shutting down application...");
            try {
                streams.close(Duration.ofMinutes(2));
                logger.info("Kafka Streams application shut down successfully");
            } catch (Exception e) {
                logger.error("Error during shutdown: {}", e.getMessage(), e);
            }
        }));
    }
}