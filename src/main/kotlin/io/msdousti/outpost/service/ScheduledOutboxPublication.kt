package io.msdousti.outpost.service


import io.msdousti.outpost.repo.AdvisoryLockRepository
import io.msdousti.outpost.repo.OutboxMessage
import io.msdousti.outpost.repo.OutboxRepository
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import javax.sql.DataSource
import kotlin.coroutines.cancellation.CancellationException
import kotlin.math.ceil


const val OUTBOX_LOCK_ID: Long = 0x5A6BF480DC06A98C

@Service
class ScheduledOutboxPublication(
    private val dataSource: DataSource,
    private val outboxRepository: OutboxRepository,
    private val kafkaPublisher: KafkaPublisher,
    @Value("\${outbox.parallelism:10}")
    private val parallelism: Int,
    @Value("\${outbox.batch-size:100}")
    private val batchSize: Int,
    // for tests
    scopeOverride: CoroutineScope? = null,
) {
    private val logger = LoggerFactory.getLogger(this::class.java)

    private val executor = Executors.newFixedThreadPool(parallelism)
    private val supervisorJob = SupervisorJob()
    private val outboxCoroutineScope = scopeOverride ?: CoroutineScope(executor.asCoroutineDispatcher() + supervisorJob)

    @Scheduled(
        fixedDelayString = "\${outbox.fixed-delay-ms:1000}",
        timeUnit = TimeUnit.MILLISECONDS,
    )
    fun publishOutboxMessages() = outboxCoroutineScope.launch {
        AdvisoryLockRepository(dataSource.connection).use { advisoryLockRepository ->
            if (!advisoryLockRepository.tryAdvisorySessionLock(OUTBOX_LOCK_ID)) {
                logger.info("Another pod is taking care of the outbox publication")
                return@launch
            }
            logger.info("Advisory lock $OUTBOX_LOCK_ID has been acquired")
            try {
                outboxRepository.fetchUnprocessedMessages(batchSize)
                    .ifEmpty { null }
                    ?.let { processOutboxMessages(it) }
            } finally {
                advisoryLockRepository.unlockAdvisorySessionLock(OUTBOX_LOCK_ID)
                logger.info("Advisory lock $OUTBOX_LOCK_ID has been released")
            }
        }
    }

    @PreDestroy
    fun preDestroy() {
        logger.info("Stopping outbox publication...")
        supervisorJob.cancel()
        executor.shutdown()
    }

    private suspend fun processOutboxMessages(outboxMessages: List<OutboxMessage>) = coroutineScope {
        val groupedMessages = outboxMessages.groupBy { it.groupingKey }.values
        // For simplicity, we assume the data is uniformly distributed in the groups
        val chunkSize = ceil(groupedMessages.size.toDouble() / parallelism).toInt().coerceAtLeast(1)

        groupedMessages.chunked(chunkSize).map { messageGroups ->
            val messages = messageGroups.flatten()
            async {
                try {
                    logger.info("Publishing $messages")
                    publishAndMarkAsProcessed(messages)
                } catch (e: CancellationException) {
                    logger.info("received CancellationException")
                    throw e
                } catch (e: Exception) {
                    logger.error("Failed to publish outbox messages", e)
                }
            }
        }.awaitAll()
    }

    private suspend fun publishAndMarkAsProcessed(messages: List<OutboxMessage>) = coroutineScope {
        messages.forEach { message ->
            coroutineContext.ensureActive()
            kafkaPublisher.publish(message)
        }
        // Note: This runs in its own transaction,
        // and the transaction boundary does not include network I/O for Kafka publication
        outboxRepository.markAsProcessed(messages.map { it.id })
    }
}
