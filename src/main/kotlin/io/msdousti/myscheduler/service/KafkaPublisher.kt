package io.msdousti.myscheduler.service

import io.msdousti.myscheduler.repo.OutboxMessage
import org.springframework.stereotype.Service

@Service
class KafkaPublisher {
    fun publish(message: OutboxMessage) {
        // Simulate blocking delay in Kafka publication
        Thread.sleep(10)
    }
}
