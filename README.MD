# Outpost (Outbox + Postgres)

![Outpost logo](./outpost-logo.jpeg)

Outpost is a Spring Boot application that implements the outbox pattern for reliable message publishing to Kafka. It
ensures high performance by incorporating Kotlin coroutines, Postgres partitioning, and general best practices.

😎 Outpost has some cool features 😎

* It uses session-level advisory locks, as opposed to transaction-level advisory locks
    * Therefore, it does not define any transaction boundaries explicitly. Each SQL statement is run in its implicit
      transactions.
    * Hence, no open transactions while doing network I/O (= publish to Kafka)
* It allows for grouping messages based on a `groupingKey`. Messages with the same `groupingKey` are published in order,
  while messages with different grouping keys are published in parallel. Parallelism is adjustable.
* Adaptive scheduling: Increase or decrease the delay between each publication depending on the result of the last round
* It is compatible with partitioned outboxes (published/unpublished). See my blog posts:
    * [PostgreSQL + Outbox Pattern Revamped — Part 1](https://dev.to/msdousti/postgresql-outbox-pattern-revamped-part-1-3lai)
    * [PostgreSQL + Outbox Pattern Revamped — Part 2](https://dev.to/msdousti/postgresql-outbox-pattern-revamped-part-2-1cbf)

## Project Overview

The project uses the following technologies:

- Spring Boot 3.5.4
- Kotlin with Coroutines
- PostgreSQL for the outbox table
- jOOQ for database access
- JDK 21

### Key Features

- **Outbox Pattern Implementation**: Reliable delivery flow implemented in `OutboxPublisherService.kt`.
- **Advisory Locks (no long-running TXNs during I/O)**: Session-level locks prevent concurrent processors across
  instances; see `AdvisoryLockRepository.kt` and lock usage in `OutboxPublisherService.kt`.
- **Parallel Processing with per-key ordering**: Work is sharded by groupingKey and processed in parallel; tune
  parallelism as needed.
- **Batch Processing**: Adjustable batch size to balance DB round-trips vs. publisher throughput.
- **Message Grouping**: Messages with the same groupingKey are published in order; different keys can progress
  concurrently.

## Running with Docker Compose

The project includes a Docker Compose configuration to set up the required PostgreSQL database:

```bash
docker compose -f docker/docker-compose.yaml up -d
```

This will start a PostgreSQL instance on port 54320 with the necessary outbox table structure.

## Running the Spring Boot Application

To run the application:

1. Make sure the PostgreSQL container is running (see above)

2. Populate the DB (optional performance demo seed):
    ```bash
    psql -h localhost -p 54320 -U postgres <<EOF
    truncate outbox;
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

- `outpost.enabled`: Enable/disable the background scheduler loop (default: true)
- `outpost.parallelism`: Number of parallel threads for processing messages (default: 10)
- `outpost.batch-size`: Number of messages to process in each batch (default: 100)

## Data Model and Performance

### Partitioned outbox schema

The outbox table is partitioned by processed_at into two partitions managed by `docker/init.sql`:

- `outbox_unpublished`: rows where processed_at IS NULL (with an index on id).
- `outbox_published`: DEFAULT partition for processed rows.

Autovacuum settings in the script bias vacuum/analyze behavior for high insert/update workloads. This layout enables:

- Fast scans of unprocessed rows (tight index range on the unpublished partition).
- Efficient movement of rows from unpublished to published with minimal bloat.

### Grouping and throughput

Messages are grouped by `grouping_key`. Within the same key, messages are published in order; different keys can be
processed concurrently. Practical tuning tips:

- `outpost.parallelism`: Increase when you have many distinct grouping keys to unlock more concurrency.
- `outpost.batch-size`: Increase to amortize DB round-trips if per-message publish cost is low; decrease if publisher is
  slow or memory is constrained.
  The scheduler applies an adaptive capped exponential backoff with jitter (cap: 1000 ms) when no work is found, and
  resets delay to 0 after a successful batch (see `src/main/kotlin/io/msdousti/outpost/scheduler/OutpostScheduler.kt`).

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

- `src/main/kotlin/io/msdousti/outpost/Application.kt`: Spring Boot application entry point
- `src/main/kotlin/io/msdousti/outpost/service/OutboxPublisherService.kt`: Core service that implements the outbox
  pattern
- `src/main/kotlin/io/msdousti/outpost/service/KafkaPublisher.kt`: Service for publishing messages to Kafka
- `src/main/kotlin/io/msdousti/outpost/repo/OutboxRepository.kt`: Repository for accessing the outbox table
- `src/main/kotlin/io/msdousti/outpost/repo/AdvisoryLockRepository.kt`: Helper for PostgreSQL advisory locks
- `src/main/kotlin/io/msdousti/outpost/scheduler/OutpostScheduler.kt`: Background scheduler that triggers publishing
- `src/main/kotlin/io/msdousti/outpost/scheduler/ExecutorConfig.kt`: Thread pool executors configuration
- `docker/docker-compose.yaml`: Docker Compose configuration for PostgreSQL
- `docker/init.sql`: SQL script to initialize the outbox table structure
