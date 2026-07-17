package io.pivotal.demo

import io.pivotal.demo.bindings.CountsSinkBinding
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.stereotype.Component

@Component
@EnableBinding(CountsSinkBinding::class)
class CountsConsumer(private val wordCountStore: WordCountStore) {

    @StreamListener(CountsSinkBinding.COUNTS_IN)
    fun consume(wordCount: WordCountMessage) {
        wordCountStore.update(wordCount.word, wordCount.wordCount)
    }
}
