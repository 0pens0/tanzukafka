package io.pivotal.demo;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import io.pivotal.demo.bindings.LinesProcessorBinding;
import io.pivotal.demo.domain.WordCount;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

@SpringBootApplication
public class WordcountApplication {

    public static void main(String[] args) {
        SpringApplication.run(WordcountApplication.class, args);
    }

    @Component
    @EnableBinding(LinesProcessorBinding.class)
    public static class LinesProcessor {
        private final Logger log = LoggerFactory.getLogger(getClass());
        /** Lowercase word of at least three characters that does not start with a digit. */
        static final String COUNTABLE_WORD = "^[a-z][a-z0-9]{2,}$";
        static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
                "about", "after", "all", "also", "and", "any", "are", "back", "been", "before", "being",
                "but", "can", "could", "did", "does", "doing", "each", "even", "for", "from", "get",
                "had", "has", "have", "here", "how", "into", "its", "just", "let", "like", "make",
                "many", "more", "most", "much", "new", "not", "now", "off", "one", "only", "other",
                "our", "out", "over", "own", "same", "see", "should", "some", "such", "than", "that",
                "the", "their", "them", "then", "there", "these", "they", "this", "those", "through",
                "too", "use", "used", "using", "very", "was", "way", "well", "were", "what", "when",
                "where", "which", "while", "who", "why", "will", "with", "would", "you", "your",
                // spring.io chrome / legal footer that sometimes still leaks into scraped text
                "affiliates", "binaries", "broadcom", "certification", "copyright", "countries",
                "guidelines", "newsletter", "offers", "privacy", "progress", "registered", "reserved",
                "rights", "subscribe", "subscription", "subsidiaries", "trademark", "trademarks",
                "training", "united", "upcoming", "vmware"));

        static boolean isCountable(String word) {
            return word.matches(COUNTABLE_WORD) && !STOP_WORDS.contains(word);
        }

        @StreamListener(LinesProcessorBinding.LINES_IN)
        @SendTo(LinesProcessorBinding.COUNTS_OUT)
        public KStream<?, WordCount> countWords(KStream<Object, String> lines) {
            KStream<?, WordCount> countStream = lines
                    .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\b")))
                    .filter((key, value) -> isCountable(value))
                    .map((key, value) -> new KeyValue<>(value, value))
                    .groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
                    .count(Materialized.as("AggregateCount"))
                    .toStream()
                    .map((w, c) -> new KeyValue<>(null, new WordCount(w, c)));

            countStream.foreach((key, wc) -> log.info("WordCount - {}", wc));
            return countStream;
        }
    }
}
