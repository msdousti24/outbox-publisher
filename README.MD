# Outpost (Outbox + Postgres)

![Outpost logo](./outpost-logo.jpeg)

Outpost is a Spring Boot application that implements the outbox pattern for reliable message publishing to Kafka. It
ensures high performance by incorporating Kotlin coroutines, Postgres partitioning, and general best practices.

## Project Overview

The project uses the following technologies:

- Spring Boot 3.5.4
- Kotlin with Coroutines
- PostgreSQL for the outbox table
- jOOQ for database access
- JDK 21

### Key Features

- **Outbox Pattern Implementation**: Ensures reliable message delivery to Kafka
- **Advisory Locks**: Prevents multiple instances from processing the same messages
- **Parallel Processing**: Configurable parallelism for efficient message processing
- **Batch Processing**: Configurable batch size for optimal throughput
- **Message Grouping**: Groups messages with the same grouping key for ordered processing

## Running with Docker Compose

The project includes a Docker Compose configuration to set up the required PostgreSQL database:

```bash
docker compose -f docker/docker-compose.yaml up -d
```

This will start a PostgreSQL instance on port 54320 with the necessary outbox table structure.

## Running the Spring Boot Application

To run the application:

1. Make sure the PostgreSQL container is running (see above)

2. Populate the DB:
    ```bash
    psql -h localhost -p 54320 -U postgres <<EOF
    insert into outbox(grouping_key) select i::text from generate_series(1,1_000_000) as i;
    analyze outbox;
    EOF
    ```

3. Run the application using Gradle:
   ```bash
   ./gradlew bootRun
   ```

   Or using your IDE's Spring Boot run configuration.

### Configuration

The application can be configured through `application.yaml`. Key configuration properties:

- `outbox.parallelism`: Number of parallel threads for processing messages (default: 10)
- `outbox.batch-size`: Number of messages to process in each batch (default: 100)
- `outbox.fixed-delay-ms`: Delay between processing batches in milliseconds (default: 1000)

## Running Tests

The project includes comprehensive tests for the outbox publication functionality:

1. Make sure the PostgreSQL container is running (see above)

2. Run the tests using Gradle:
    ```bash
    ./gradlew test
    ```

Or using your IDE's test runner.

### Test Categories

- **Integration Tests**: Test the full outbox publication flow with a real PostgreSQL database
- **Advisory Lock Tests**: Verify the behavior of advisory locks
- **Message Grouping Tests**: Ensure messages with the same grouping key are processed together

## Project Structure

- `src/main/java/io/msdousti/outpost/service/ScheduledOutboxPublication.kt`: Core service that implements the outbox
  pattern
- `src/main/java/io/msdousti/outpost/service/KafkaPublisher.kt`: Service for publishing messages to Kafka
- `src/main/java/io/msdousti/outpost/repo/OutboxRepository.kt`: Repository for accessing the outbox table
- `docker/docker-compose.yaml`: Docker Compose configuration for PostgreSQL
- `docker/init.sql`: SQL script to initialize the outbox table structure
