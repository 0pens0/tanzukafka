package io.pivotal.demo

import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController

data class Word(val text: String, val weight: Long)

class CompareWords {
    companion object : Comparator<Word> {
        override fun compare(a: Word, b: Word): Int = b.weight.compareTo(a.weight)
    }
}

@RestController
class WordCloudAPI(private val wordCountStore: WordCountStore) {

    @GetMapping("/words", "/words/60S")
    fun words(): List<Word> = toWords(wordCountStore.topWords())

    @GetMapping("/words/2-5M")
    fun wordsWindowed(): List<Word> = toWords(wordCountStore.topWords())

    private fun toWords(wordsHashMap: Map<String, Long>): List<Word> {
        val allwords = wordsHashMap.map { (word, count) -> Word(word, count) }.toMutableList()
        if (allwords.isEmpty()) {
            allwords.add(Word("NO STREAM DATA", 10))
        }
        return allwords.sortedWith(CompareWords).take(20)
    }
}
