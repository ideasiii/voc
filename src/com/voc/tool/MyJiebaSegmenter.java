package com.voc.tool;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode;
import com.huaban.analysis.jieba.SegToken;

/**
 * 结巴分词(java版) :  ==>https://github.com/huaban/jieba-analysis
 * - 當前快照版本: 1.0.3-SNAPSHOT
 * - 當前穩定版本: 1.0.2 ==>這邊先使用這個版本: jieba-analysis-1.0.2.jar (使用 Maven Install 包裝起來)
 * 
 * 支援分詞模式
 * - Search模式，用於對使用者查詢詞分詞: ==>這邊先使用這個模式!!!
 * - Index模式，用於對索引文檔分詞
 * 
 * Note: 其中 dict.txt (字典檔) 替換成繁體版: 
 * - 下載來源:  ==>https://github.com/fukuball/Head-first-Chinese-text-segmentation/tree/master/data
 * 
 */
public class MyJiebaSegmenter {
	private static final Logger LOGGER = LoggerFactory.getLogger(MyJiebaSegmenter.class);
	private static final MyJiebaSegmenter instance = new MyJiebaSegmenter();
	private JiebaSegmenter segmenter;
	
	private MyJiebaSegmenter() {
		LOGGER.info("Init JiebaSegmenter Start...");
		this.segmenter = new JiebaSegmenter();
		LOGGER.info("Init JiebaSegmenter Finish!");
	}
	
	public static MyJiebaSegmenter getInstance() {
		return instance;
	}
	
	public List<SegToken> process(String sentence) {
		return this.segmenter.process(sentence, SegMode.SEARCH);
		// return this.segmenter.process(sentence, SegMode.INDEX);
	}
	
	// ***************************************************************************
	
	/**
	 * Jieba Test: 
	 */
	public static void main(String[] args) {
		// String sentence = "各位媽媽千萬小心啊∼  #腸病毒 #護理之家";
		// String sentence = "各位媽媽千萬小心啊∼#腸病毒#護理之家";
		// String sentence = "就忙著去煮飯了";
		String sentence = "一起去旅行";
		// String sentence = "老虎吃肉";
		
		List<SegToken> tokens = MyJiebaSegmenter.getInstance().process(sentence);
		for (SegToken segToken : tokens) {
			String word = segToken.word;
			System.out.println("word = " + word);
		}
		
	}

}
