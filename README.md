# Kafka Streams Data Processing Pipeline

This project implements a modular data processing pipeline using Kafka Streams for handling CDC (Change Data Capture) events.

## Architecture

The pipeline consists of multiple modules:

- **Common**: Shared utilities and models used across the pipeline components
- **Preprocessor**: Initial pipeline component that validates, transforms, and routes events

## Prerequisites

- Java 11 or higher
- Apache Maven 3.6.0 or higher
- Kafka cluster (version 3.0.0 or higher)

## Building the Project

To build the project, run the following command from the project root directory:

```bash
mvn clean package
```

This will compile the code, run tests, and create executable JAR files for each module.

## Running the Pipeline

### Configuring the Environment

Before running the pipeline, make sure you have:

1. A running Kafka cluster
2. The required Kafka topics created:
   - Input topics (configured in `preprocessor.conf`)
   - Output topics (these will be auto-created if `auto.create.topics.enable` is enabled in Kafka)

### Running the Preprocessor

To start the preprocessor component:

```bash
java -jar pipeline/preprocessor/target/preprocessor-0.1.0-jar-with-dependencies.jar
```

### Configuration

The application uses the following configuration files:

- `preprocessor.conf`: Main configuration file for the preprocessor
- `logback.xml`: Logging configuration

Configuration can be overridden by placing custom config files in `/data/conf/`.

## Configuration Options

### Preprocessor Configuration (`preprocessor.conf`)

```hocon
kafka {
  application.id = "kafka-streams-preprocessor"
  bootstrap.servers = "localhost:9092"
  numThreads = 2

  topics {
    inputs = ["input1", "input2", "input3"]
    output_suffix = "_canonical"
    failed = "failed"
  }
}

schemas {
    i = "/event-schema.json"
    d = "/event-schema.json"
    a = "/update.json"
}
```

## Pipeline Flow

1. **Validation**: Incoming events are validated against JSON schemas
2. **Canonicalization**: Valid events are transformed into a canonical format
3. **Partitioning**: Events are routed to appropriate output topics based on their table name

## Troubleshooting

- Check logs in the console or configured log directory
- Failed events are sent to the configured failed topic for debugging
- Set log level to DEBUG in `logback.xml` for more detailed logging

## Development

### Adding New Components

To add new pipeline components:

1. Create a new module under the pipeline directory
2. Add it to the parent POM's modules section
3. Implement the processing logic using Kafka Streams APIs

### Extending Schemas

Schema files are located in `src/main/resources` of the preprocessor module.

## License

Copyright © 2023 Sanketika
