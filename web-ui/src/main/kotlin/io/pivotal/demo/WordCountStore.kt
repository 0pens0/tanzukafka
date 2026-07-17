package io.pivotal.demo

import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap

@Component
class WordCountStore {

    private val counts = ConcurrentHashMap<String, Long>()

    fun update(word: String?, count: Long) {
        if (word == null || word.isBlank()) {
            return
        }
        counts.put(word, count)
    }

    fun topWords(limit: Int = 20): Map<String, Long> {
        return counts.entries
            .sortedByDescending { it.value }
            .take(limit)
            .associate { it.key to it.value }
    }

    fun size(): Int = counts.size
}
