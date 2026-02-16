package io.msdousti.outpost.service

import io.msdousti.outpost.repo.OutboxMessage
import io.msdousti.outpost.repo.OutboxRepository
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import kotlin.coroutines.cancellation.CancellationException
import kotlin.math.ceil

@Service
class OutboxPublisherService(
    private val outboxRepository: OutboxRepository,
    private val kafkaPublisher: KafkaPublisher,
    @Value($$"${outpost.parallelism}")
    private val parallelism: Int,
    @Value($$"${outpost.batch-size}")
    private val batchSize: Int,
) {
    private val logger = LoggerFactory.getLogger(this::class.java)

    suspend fun publishOutboxMessages(): Boolean {
        val messages = outboxRepository.fetchUnprocessedMessages(batchSize)
        return if (messages.isEmpty())
            false
        else {
            processOutboxMessages(messages)
            true
        }
    }

    private suspend fun processOutboxMessages(outboxMessages: List<OutboxMessage>) = coroutineScope {
        val groupedMessages = outboxMessages.groupBy { it.groupingKey }.values
        // For simplicity, we assume the data is uniformly distributed in the groups
        val chunkSize = ceil(groupedMessages.size.toDouble() / parallelism).toInt().coerceAtLeast(1)

        groupedMessages.chunked(chunkSize).forEach { messageGroups ->
            val messages = messageGroups.flatten()
            launch {
                try {
                    logger.debug("Publishing {}", messages)
                    publishAndMarkAsProcessed(messages)
                } catch (ce: CancellationException) {
                    logger.debug("received CancellationException")
                    throw ce
                } catch (e: Exception) {
                    logger.error("Failed to publish outbox messages", e)
                }
            }
        }
    }

    private suspend fun publishAndMarkAsProcessed(messages: List<OutboxMessage>) {
        messages.forEach { message ->
            currentCoroutineContext().ensureActive()
            kafkaPublisher.publish(message)
        }
        // Note: This runs in its own transaction,
        // and the transaction boundary does not include network I/O for Kafka publication
        outboxRepository.markAsProcessed(messages.map { it.id })
    }
}
