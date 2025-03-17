package com.sanketika.pipeline.preprocessor.task;

import com.sanketika.common.models.ErrorEvent;
import com.sanketika.common.models.ProcessingResult;
import com.sanketika.common.util.JsonUtil;
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

import java.time.Duration;
import java.util.Properties;

public class PreprocessorTask {

    public static void main(String[] args) {
        try {
            Properties props = createKafkaProperties();
            StreamsBuilder builder = new StreamsBuilder();

            buildStreamsTopology(builder);

            final KafkaStreams streams = new KafkaStreams(builder.build(), props);

            // Clean up state on shutdown
            streams.setStateListener((newState, oldState) -> {
                System.out.println("Kafka Streams state changed from " + oldState + " to " + newState);
            });

            // Handle uncaught exceptions
            streams.setUncaughtExceptionHandler((thread, throwable) -> {
                System.err.println("Uncaught exception in Kafka Streams thread " + thread + ": " + throwable.getMessage());
                throwable.printStackTrace();
            });

            // Setup shutdown hook
            setupShutdownHook(streams);

            try {
                streams.cleanUp(); // Clean local state stores before starting
                streams.start();
                System.out.println("Kafka Streams application started, listening for messages on topic: " + PreProcessorConfig.inputTopic);

                // Keep application running indefinitely
                // No need to wait on a latch - the application will continue to run
                // The main thread will stay alive as long as non-daemon threads (like those in Kafka Streams) are running

            } catch (Exception e) {
                System.err.println("Error during streams execution: " + e.getMessage());
                e.printStackTrace();
                streams.close(Duration.ofMinutes(1));
            }

        } catch (Exception e) {
            System.err.println("Failed to start Kafka Streams application: " + e.getMessage());
            e.printStackTrace();
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

        System.out.println("Kafka configuration: " + props);

        return props;
    }

    private static void buildStreamsTopology(StreamsBuilder builder) {
        String inputTopic = PreProcessorConfig.inputTopic;

        // Explicitly specify serdes for input topic
        KStream<String, String> sourceStream = builder.stream(
            inputTopic,
            Consumed.with(Serdes.String(), Serdes.String())
        );

        // Add logging for consumed messages
//        sourceStream = sourceStream.peek((key, value) -> {
//            System.out.println("=========================================================");
//            System.out.println("[INPUT MESSAGE] Topic: " + inputTopic);
//            System.out.println("[INPUT MESSAGE] Key: " + key);
//            System.out.println("[INPUT MESSAGE] Value: " + value);
//            System.out.println("=========================================================");
//        });

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
        System.out.println("Built the following topology:");
        System.out.println("Input topic: " + inputTopic);
        System.out.println("Output topic: " + PreProcessorConfig.outputTopic);
        System.out.println("Failed topic: " + PreProcessorConfig.failedTopic);
    }

    private static void sinkEvents(KStream<String, ProcessingResult> stream) {
        String topic = PreProcessorConfig.outputTopic;

        stream
                .mapValues(ProcessingResult::getPayload)
                .peek((key, value) -> System.out.println("[Success Branch] Key: " + key + ", Value: " + value))
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
                    .peek((key, value) -> System.out.println("[Failed Branch] Key: " + key + ", Value: " + value))
                    .to(topic, Produced.with(Serdes.String(), Serdes.String()));
        }
    }

    private static void setupShutdownHook(KafkaStreams streams) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown hook triggered - gracefully shutting down application...");
            try {
                // Request closing of streams
                streams.close(Duration.ofMinutes(5)); // Increased timeout to 5 minutes
                System.out.println("Kafka Streams application shut down successfully");
            } catch (Exception e) {
                System.err.println("Error during shutdown: " + e.getMessage());
                e.printStackTrace();
            }
        }));
    }
}