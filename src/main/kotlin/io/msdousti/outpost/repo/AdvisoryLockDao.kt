package io.msdousti.outpost.repo

import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.sql.Connection
import javax.sql.DataSource

private class AdvisoryLockDao(private val connection: Connection) : AutoCloseable {
    private val dslContext: DSLContext = DSL.using(connection, SQLDialect.POSTGRES)

    fun tryAdvisorySessionLock(lockId: Long): Boolean =
        dslContext.fetchValue("SELECT pg_try_advisory_lock($lockId)") as Boolean

    fun unlockAdvisorySessionLock(lockId: Long) {
        dslContext.fetchValue("SELECT pg_advisory_unlock($lockId)")
    }

    override fun close() {
        connection.close()
    }
}

@Component
class AdvisoryLockService(private val dataSource: DataSource) {
    private val logger = LoggerFactory.getLogger(this::class.java)

    suspend fun wrapInSessionLock(lockId: Long, lambda: suspend () -> Boolean): Boolean {
        AdvisoryLockDao(dataSource.connection).use { advisoryLockRepository ->
            if (!advisoryLockRepository.tryAdvisorySessionLock(lockId)) {
                logger.debug("Another process is taking care of the outbox publication")
                return false
            }
            try {
                logger.info("Advisory lock {} has been acquired", lockId)
                return lambda()
            } finally {
                advisoryLockRepository.unlockAdvisorySessionLock(lockId)
                logger.info("Advisory lock {} has been released", lockId)
            }
        }
    }
}
