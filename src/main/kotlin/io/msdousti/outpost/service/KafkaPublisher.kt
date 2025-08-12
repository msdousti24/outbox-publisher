package io.msdousti.outpost.service

import io.msdousti.outpost.repo.OutboxMessage
import org.springframework.stereotype.Service

@Service
class KafkaPublisher {
    fun publish(message: OutboxMessage) {
        // Simulate blocking delay in Kafka publication
        Thread.sleep(1)
    }
}
