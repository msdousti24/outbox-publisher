package io.msdousti.myscheduler.repo

import org.jooq.DSLContext
import org.springframework.stereotype.Repository

@Repository
class OutboxRepository(private val dslContext: DSLContext) {
    fun fetchUnprocessedMessages(outboxProcessingBatchSize: Int): List<OutboxMessage> {
        return dslContext.fetch(
            """
            SELECT id, grouping_key
            FROM outbox
            WHERE processed_at IS NULL
            ORDER BY id
            LIMIT $outboxProcessingBatchSize
            """.trimIndent()
        ).map { OutboxMessage(it[0] as Long, it[1] as String) }
    }

    fun markAsProcessed(ids: List<Long>) {
        dslContext.execute("UPDATE outbox SET processed_at = now() WHERE id IN (${ids.joinToString(",")})")
    }

}

data class OutboxMessage(val id: Long, val groupingKey: String)
