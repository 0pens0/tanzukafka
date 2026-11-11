package io.pivotal.demo;

import org.junit.Test;

import static io.pivotal.demo.WordcountApplication.LinesProcessor.isCountable;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LinesProcessorFilterTests {

	@Test
	public void countsMeaningfulWords() {
		assertTrue(isCountable("spring"));
		assertTrue(isCountable("kafka"));
		assertTrue(isCountable("java21"));
	}

	@Test
	public void skipsStopWords() {
		assertFalse(isCountable("the"));
		assertFalse(isCountable("and"));
		assertFalse(isCountable("with"));
		assertFalse(isCountable("trademarks"));
		assertFalse(isCountable("copyright"));
	}

	@Test
	public void skipsShortTokensNumbersAndPunctuation() {
		assertFalse(isCountable("a"));
		assertFalse(isCountable("of"));
		assertFalse(isCountable("2026"));
		assertFalse(isCountable("3rd"));
		assertFalse(isCountable(" "));
		assertFalse(isCountable("."));
	}
}
