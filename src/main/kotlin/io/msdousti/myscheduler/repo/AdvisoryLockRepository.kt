package io.msdousti.myscheduler.repo

import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import java.sql.Connection

class AdvisoryLockRepository(private val connection: Connection): AutoCloseable {
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
