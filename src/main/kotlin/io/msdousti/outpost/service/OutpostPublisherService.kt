package io.msdousti.outpost.service

import io.msdousti.outpost.repo.AdvisoryLockRepository
import io.msdousti.outpost.repo.OutboxMessage
import io.msdousti.outpost.repo.OutboxRepository
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.ensureActive
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import javax.sql.DataSource
import kotlin.coroutines.cancellation.CancellationException
import kotlin.math.ceil

const val OUTBOX_LOCK_ID: Long = 0x5A6BF480DC06A98C

@Service
class OutboxPublisherService(
    private val dataSource: DataSource,
    private val outboxRepository: OutboxRepository,
    private val kafkaPublisher: KafkaPublisher,
    @Value("\${outpost.parallelism:10}")
    private val parallelism: Int,
    @Value("\${outpost.batch-size:100}")
    private val batchSize: Int,
) {
    private val logger = LoggerFactory.getLogger(this::class.java)

    suspend fun publishOutboxMessages(): Boolean {
        AdvisoryLockRepository(dataSource.connection).use { advisoryLockRepository ->
            if (!advisoryLockRepository.tryAdvisorySessionLock(OUTBOX_LOCK_ID)) {
                logger.info("Another process is taking care of the outbox publication")
                return false
            }
            logger.info("Advisory lock {} has been acquired", OUTBOX_LOCK_ID)
            try {
                val messages = outboxRepository.fetchUnprocessedMessages(batchSize)
                if (messages.isEmpty()) return false
                processOutboxMessages(messages).awaitAll()
            } finally {
                advisoryLockRepository.unlockAdvisorySessionLock(OUTBOX_LOCK_ID)
                logger.info("Advisory lock {} has been released", OUTBOX_LOCK_ID)
            }
            return true
        }
    }

    private suspend fun processOutboxMessages(outboxMessages: List<OutboxMessage>) = coroutineScope {
        val groupedMessages = outboxMessages.groupBy { it.groupingKey }.values
        // For simplicity, we assume the data is uniformly distributed in the groups
        val chunkSize = ceil(groupedMessages.size.toDouble() / parallelism).toInt().coerceAtLeast(1)

        groupedMessages.chunked(chunkSize).map { messageGroups ->
            val messages = messageGroups.flatten()
            async {
                try {
                    logger.debug("Publishing {}", messages)
                    publishAndMarkAsProcessed(messages).also {
                        logger.info("Published {} messages", it)
                    }
                } catch (ce: CancellationException) {
                    logger.info("received CancellationException")
                    throw ce
                } catch (e: Exception) {
                    logger.error("Failed to publish outbox messages", e)
                }
            }
        }
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
