@file:Suppress("SpringBootApplicationProperties")

package io.msdousti.myscheduler.service

import com.ninjasquad.springmockk.MockkBean
import com.ninjasquad.springmockk.SpykBean
import io.kotest.matchers.longs.shouldBeGreaterThan
import io.kotest.matchers.maps.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.mockk.clearAllMocks
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.just
import io.mockk.runs
import io.mockk.slot
import io.mockk.verify
import io.mockk.verifyOrder
import io.msdousti.myscheduler.repo.AdvisoryLockRepository
import io.msdousti.myscheduler.repo.OutboxMessage
import io.msdousti.myscheduler.repo.OutboxRepository
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceUntilIdle
import org.jooq.DSLContext
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.test.context.ActiveProfiles
import javax.sql.DataSource

private const val BATCH_SIZE = 100
private const val PARALLELISM = 4

@OptIn(ExperimentalCoroutinesApi::class)
@ActiveProfiles("test")
@SpringBootTest(
    properties = [
        "outbox.batch-size = $BATCH_SIZE",
        "outbox.parallelism = $PARALLELISM"
    ]
)
class ScheduledOutboxPublicationIT {

    @TestConfiguration
    class CoroutineScopeTestConfig {
        @Bean
        fun testCoroutineScope() = TestScope(StandardTestDispatcher())
    }

    @Autowired
    lateinit var dataSource: DataSource

    @Autowired
    lateinit var dslContext: DSLContext

    @MockkBean
    lateinit var kafkaPublisher: KafkaPublisher

    @SpykBean
    lateinit var outboxRepository: OutboxRepository

    @Autowired
    lateinit var service: ScheduledOutboxPublication

    @Autowired
    lateinit var testScope: TestScope

    @BeforeEach
    fun cleanAndSeed() {
        clearAllMocks()
        every { kafkaPublisher.publish(any()) } just runs
        dslContext.execute("TRUNCATE outbox")
    }

    @Test
    fun `does nothing when advisory lock is held in another session`() {
        insertMessages(1, 1)

        AdvisoryLockRepository(dataSource.connection).use { advisoryLockRepository ->
            if (!advisoryLockRepository.tryAdvisorySessionLock(OUTBOX_LOCK_ID))
                throw IllegalStateException("advisory lock cannot be acquired")

            runCatching {
                publishOutboxAndReturnCountProcessed() shouldBe 0
            }

            advisoryLockRepository.unlockAdvisorySessionLock(OUTBOX_LOCK_ID)
        }
    }

    @Test
    fun `publishes and marks processed when lock is free`() {
        insertMessages(1, 10 * BATCH_SIZE)

        repeat(BATCH_SIZE / 10) {
            publishOutboxAndReturnCountProcessed() shouldBe BATCH_SIZE * (it + 1)
            verify(exactly = BATCH_SIZE) { kafkaPublisher.publish(any()) }

            verify(exactly = 1) { outboxRepository.fetchUnprocessedMessages(BATCH_SIZE) }
            verify(exactly = PARALLELISM) { outboxRepository.markAsProcessed(any()) }

            confirmVerified(kafkaPublisher)
        }

        publishOutboxAndReturnCountProcessed() shouldBe 10 * BATCH_SIZE
        verify(exactly = 1) { outboxRepository.fetchUnprocessedMessages(BATCH_SIZE) }
        verify(exactly = 0) { kafkaPublisher.publish(any()) }
        verify(exactly = 0) { outboxRepository.markAsProcessed(any()) }
    }

    @Test
    fun `groups messages with the same grouping_key`() {
        repeat(2) { insertMessages(1, BATCH_SIZE / 2) }
        repeat(2) { insertMessages(BATCH_SIZE / 2 + 1, BATCH_SIZE) }

        val slots = List(2) { List(BATCH_SIZE) { slot<OutboxMessage>() } }

        repeat(2) { i ->
            publishOutboxAndReturnCountProcessed() shouldBe BATCH_SIZE * (i + 1)
            verifyOrder {
                slots[i].forEach {
                    kafkaPublisher.publish(capture(it))
                }
            }

            verify(exactly = 1) { outboxRepository.fetchUnprocessedMessages(BATCH_SIZE) }
            verify(exactly = PARALLELISM) { outboxRepository.markAsProcessed(any()) }

            confirmVerified(kafkaPublisher)
        }

        val data = slots.map { slot ->
            slot.map { it.captured }
        }

        data.forEach { rows ->
            val grp = rows.groupBy { it.groupingKey }
            grp shouldHaveSize BATCH_SIZE / 2
            grp.values.forEach {
                it.size shouldBe 2
                it[1].id shouldBeGreaterThan it[0].id
            }
        }

        val keys0 = data[0].map { it.groupingKey }.toSet()
        val keys1 = data[1].map { it.groupingKey }.toSet()
        keys0 intersect keys1 shouldBe emptySet()
    }

    private fun insertMessages(start: Int, end: Int) {
        @Suppress("SqlSignature")
        dslContext.execute("INSERT INTO outbox(grouping_key) SELECT i::text FROM generate_series($start, $end) AS i")
    }

    private fun publishOutboxAndReturnCountProcessed(): Int {
        service.publishOutboxMessages()
        testScope.advanceUntilIdle()

        return dslContext.fetchSingle(
            "SELECT COUNT(*) FROM outbox WHERE processed_at IS NOT NULL"
        ).into(Int::class.java)
    }
}
