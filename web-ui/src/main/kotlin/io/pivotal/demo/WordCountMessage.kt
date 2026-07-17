package io.pivotal.demo

data class WordCountMessage(
    val word: String? = null,
    val wordCount: Long = 0
)
