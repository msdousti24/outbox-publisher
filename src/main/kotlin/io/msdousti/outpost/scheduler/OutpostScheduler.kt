package io.msdousti.outpost.scheduler

import io.msdousti.outpost.repo.AdvisoryLockService
import io.msdousti.outpost.service.OutboxPublisherService
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.plus
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import org.springframework.stereotype.Component
import kotlin.math.max
import kotlin.math.min
import kotlin.random.Random

const val OUTBOX_LOCK_ID: Long = 0x5A6BF480DC06A98C

@ConditionalOnProperty(name = ["outpost.enabled"], havingValue = "true", matchIfMissing = true)
@Component
class OutpostScheduler(
    private val publisherService: OutboxPublisherService,
    private val advisoryLockService: AdvisoryLockService,
    schedulerExecutor: ThreadPoolTaskExecutor,
    outpostExecutor: ThreadPoolTaskExecutor,
) : ApplicationRunner {

    private val logger = LoggerFactory.getLogger(this::class.java)

    private val schedulerScope = CoroutineScope(schedulerExecutor.asCoroutineDispatcher()) + SupervisorJob()
    private val outpostContext = outpostExecutor.asCoroutineDispatcher() + SupervisorJob()

    @Volatile
    private var backoffMs = 0L

    @PreDestroy
    fun stopScheduler() {
        logger.debug("Stopping scheduler")
        schedulerScope.cancel()
    }

    override fun run(args: ApplicationArguments) {
        schedulerScope.launch {
            logger.debug("Worker loop started")

            while (isActive) {
                val success = try {
                    advisoryLockService.wrapInSessionLock(OUTBOX_LOCK_ID) {
                        withContext(outpostContext) {
                            publisherService.publishOutboxMessages()
                        }
                    }
                } catch (ce: CancellationException) {
                    throw ce
                } catch (t: Throwable) {
                    logger.error("Batch failed; applying backoff", t)
                    // treat as false to avoid hot loop on errors
                    false
                }
                cappedExponentialDelay(success)
            }
        }
    }

    private suspend fun cappedExponentialDelay(publishedAny: Boolean) {
        val jitter = Random.nextLong(0, 100)
        backoffMs = if (publishedAny) 0 else min(1000, max(1, backoffMs * 2) + jitter)
        logger.info("Delaying for {} ms", backoffMs)
        delay(backoffMs)
    }
}

