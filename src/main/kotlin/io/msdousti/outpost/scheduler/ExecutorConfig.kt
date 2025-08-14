package io.msdousti.outpost.scheduler

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor

@Configuration
class ExecutorConfig {
    @Bean
    fun schedulerExecutor(): ThreadPoolTaskExecutor = ThreadPoolTaskExecutor().apply {
        corePoolSize = 1
        maxPoolSize = 1
        setThreadNamePrefix("scheduler-")
        // key for graceful shutdown
        setWaitForTasksToCompleteOnShutdown(true)
        setAwaitTerminationSeconds(3)
        initialize()
    }

    @Bean
    fun outpostExecutor(
        @Value("\${outpost.parallelism:10}")
        parallelism: Int,
    ): ThreadPoolTaskExecutor = ThreadPoolTaskExecutor().apply {
        corePoolSize = parallelism
        maxPoolSize = parallelism
        setThreadNamePrefix("outpost-")
        // key for graceful shutdown
        setWaitForTasksToCompleteOnShutdown(true)
        setAwaitTerminationSeconds(1)
        initialize()
    }
}
