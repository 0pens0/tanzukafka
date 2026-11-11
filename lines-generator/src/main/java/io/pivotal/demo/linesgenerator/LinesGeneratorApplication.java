package io.pivotal.demo.linesgenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.pivotal.demo.linesgenerator.bindings.CountsSinkBinding;
import io.pivotal.demo.linesgenerator.bindings.LinesSourceBinding;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

@SpringBootApplication
public class LinesGeneratorApplication {

	public static void main(String[] args) {
		SpringApplication.run(LinesGeneratorApplication.class, args);
	}

	@Component
	@EnableBinding(LinesSourceBinding.class)
	public static class LinesGenerator implements ApplicationRunner {
		static final String BLOG_INDEX_URL = "https://spring.io/blog";
		static final String POST_LINK_SELECTOR = "a[href^=/blog/20]";
		// spring.io dropped the .blog-post wrapper; post body lives under .markdown.
		static final String POST_BODY_SELECTOR = ".markdown p";

		private final Logger log = LoggerFactory.getLogger(getClass());

		private final MessageChannel linesOut;
		public LinesGenerator(LinesSourceBinding binding) {
			linesOut = binding.linesOut();
		}

		@Override
		public void run(ApplicationArguments args) {

			Random random = new Random();
			List<String> urls = new ArrayList<>();

			Runnable runnable = () -> {
				try {
				if (urls.isEmpty()) {
					urls.addAll(fetchPostUrls());
					if (urls.isEmpty()) {
						log.warn("No blog posts found at {}", BLOG_INDEX_URL);
						return;
					}
				}
				Document doc = Jsoup.connect(urls.get(random.nextInt(urls.size()))).get();
				// Only the post body; site chrome/footer repeats on every page and drowns the cloud.
				Elements paragraphs = doc.select(POST_BODY_SELECTOR);

				for (Element paragraph : paragraphs) {
						String line = paragraph.text().trim();
						if (line.isEmpty() || isSiteChrome(line)) {
							continue;
						}
						Message<String> message = MessageBuilder
								.withPayload(line)
								.setHeader(KafkaHeaders.MESSAGE_KEY, line.getBytes())
								.build();
						linesOut.send(message);
						log.info("Sent - " + line);
					}

				} catch (Exception e) {
					log.error(e.getMessage());
				}
			};
			Executors.newScheduledThreadPool(1).scheduleAtFixedRate(runnable, 1, 3, TimeUnit.SECONDS);
		}

		/** Drop spring.io chrome that sometimes still lands in .markdown. */
		static boolean isSiteChrome(String line) {
			String lower = line.toLowerCase();
			return lower.startsWith("copyright")
					|| lower.startsWith("stay connected with the spring newsletter")
					|| lower.equals("subscribe")
					|| lower.startsWith("vmware offers training")
					|| lower.startsWith("truesource spring enterprise")
					|| lower.startsWith("check out all the upcoming events")
					|| lower.contains("are trademarks or registered trademarks");
		}

		private List<String> fetchPostUrls() throws IOException {
			Document index = Jsoup.connect(BLOG_INDEX_URL).get();
			log.info(index.title());
			return index.select(POST_LINK_SELECTOR).stream()
					.map(link -> link.absUrl("href"))
					.distinct()
					.collect(Collectors.toList());
		}

	}

	@Component
	@EnableBinding(CountsSinkBinding.class)
	public static class CountDisplay {
		private final Logger log = LoggerFactory.getLogger(getClass());

		@StreamListener(CountsSinkBinding.COUNTS_IN)
		public void showCounts(Object wc) {
			log.info("WordCount received: " + wc);
		}
	}

}
