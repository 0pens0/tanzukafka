package io.pivotal.demo.bindings

import org.springframework.cloud.stream.annotation.Input
import org.springframework.messaging.SubscribableChannel

interface CountsSinkBinding {

    companion object {
        const val COUNTS_IN = "counts-in"
    }

    @Input(COUNTS_IN)
    fun countsIn(): SubscribableChannel
}
