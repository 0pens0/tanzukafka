package io.pivotal.demo.domain;

public class WordCount {

    private String word;
    private long wordCount;

    public WordCount() {
    }

    public WordCount(String word, long count) {
        this.word = word;
        this.wordCount = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public long getWordCount() {
        return wordCount;
    }

    public void setWordCount(long count) {
        this.wordCount = count;
    }

    @Override
    public String toString() {
        return "WordCount{word='" + word + "', count=" + wordCount + '}';
    }
}
