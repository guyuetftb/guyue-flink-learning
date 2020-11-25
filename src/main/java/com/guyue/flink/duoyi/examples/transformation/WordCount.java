package com.guyue.flink.duoyi.examples.transformation;

/**
 * @ClassName WordCount
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-03 17:29
 */
public class WordCount {

	private String word;
	private Integer freq;

	public WordCount() {
	}

	public WordCount(String word, Integer freq) {
		this.word = word;
		this.freq = freq;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public Integer getFreq() {
		return freq;
	}

	public void setFreq(Integer freq) {
		this.freq = freq;
	}

	@Override
	public String toString() {
		return "WordCount{" +
			"word='" + word + '\'' +
			", freq=" + freq +
			'}';
	}
}
