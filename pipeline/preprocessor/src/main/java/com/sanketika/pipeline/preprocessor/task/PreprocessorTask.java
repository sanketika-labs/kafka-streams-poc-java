package com.sanketika.pipeline.preprocessor.task;

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
import java.util.Properties;

public class PreprocessorTask {
    private static final Logger logger = LoggerFactory.getLogger(PreprocessorTask.class);

    public static void main(String[] args) {
        // First make sure we see something with System.out
        System.out.println("Starting PreprocessorTask...");

        // Configure Logback
        LogbackConfigurer.configureLogback();

        // Test logging immediately after configuration
        // System.out.println("Testing logs after Logback configuration...");
        // logger.info("TEST INFO LOG - If you see this, logging is working");
        // logger.debug("TEST DEBUG LOG - If you see this, debug logging is enabled");
        // logger.error("TEST ERROR LOG - Should always be visible");

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
                logger.info("Kafka Streams application started, listening for messages on topic: {}", PreProcessorConfig.inputTopic);
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
        String inputTopic = PreProcessorConfig.inputTopic;

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

        sinkEvents(partitionBalancerBranchedStream);
        processFailedStreams(new KStream[]{validatorFailureStream, canonicalizerFailureStream});

        // Log topology for debugging
        logger.info("Built the following topology:");
        logger.info("Input topic: {}", inputTopic);
        logger.info("Output topic: {}", PreProcessorConfig.outputTopic);
        logger.info("Failed topic: {}", PreProcessorConfig.failedTopic);
    }

    private static void sinkEvents(KStream<String, ProcessingResult> stream) {
        String topic = PreProcessorConfig.outputTopic;

        stream
                .mapValues(ProcessingResult::getPayload)
                .peek((key, value) -> logger.debug("[Success Branch] Key: {}, Value: {}", key, value))
                .to(topic, Produced.with(Serdes.String(), Serdes.String()));
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
                // Request closing of streams
                streams.close(Duration.ofMinutes(5)); // Increased timeout to 5 minutes
                logger.info("Kafka Streams application shut down successfully");
            } catch (Exception e) {
                logger.error("Error during shutdown: {}", e.getMessage(), e);
            }
        }));
    }
}