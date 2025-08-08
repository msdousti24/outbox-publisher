package io.msdousti.outpost.config

import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.scheduling.annotation.EnableScheduling

@Profile("!test")
@Configuration
@EnableScheduling
class SchedulingConfig
