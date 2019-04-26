package com.voc.api.topic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huaban.analysis.jieba.SegToken;
import com.voc.api.RootAPI;
import com.voc.api.model.ArticleModel;
import com.voc.common.ApiResponse;
import com.voc.common.Common;
import com.voc.common.DBUtil;
import com.voc.tool.MyJiebaSegmenter;

/**
 * 查詢自訂主題文字雲: 查詢時間區間內各項目交叉分析的關鍵詞文章數
 * /topic/hot-keyword.jsp
 * 
 * EX-1:
 * - URL:
 *   ==>http://localhost:8080/voc/topic/hot-keyword.jsp?user=tsmc&project_name=自訂汽車專案&topic=日系汽車;歐系汽車&website=PTT;Mobile01&start_date=2018-12-01&end_date=2018-12-01 
 *   
 *   
 * EX-2:
 * - URL:
 *   ==>http://localhost:8080/voc/topic/hot-keyword.jsp?user=5bff9b81617d854c850a0d15&project_name=自訂汽車專案&sentiment=1;0;-1&start_date=2019-01-01&end_date=2019-12-31   
 *   
 * - Step1(SQL1):
 *   ==>SELECT id AS post_id FROM ibuzz_voc.topic_reputation 
 *      WHERE user = '5bff9b81617d854c850a0d15' AND project_name = '自訂汽車專案' 
 *      AND sentiment IN ('1','0','-1') 
 *      AND DATE_FORMAT(date, '%Y-%m-%d') >= '2019-01-01' AND DATE_FORMAT(date, '%Y-%m-%d') <= '2019-12-31';
 *      
 * - Step2:
 *   ==>queryArticleList
 *   
 * - Step3:
 *   ==>MyJiebaSegmenter
 *   
 */
public class HotKeyword extends RootAPI {
	private static final Logger LOGGER = LoggerFactory.getLogger(HotKeyword.class);
	
	private String user; // 欲查詢產業別
	private String project_name; // 欲查詢品牌
	
	private String topic;
	private String source;
	private String website;
	private String channel;
	private String sentiment;
	
	private String startDate; // start_date
	private String endDate; // end_date
	
	private int limit = 10; // Default: 10
	
	// 多個參數值
	private String[] topicValueArr;
	private String[] sourceValueArr;
	private String[] websiteNameValueArr;
	private String[] channelIdValueArr;
	private String[] sentimentValueArr;
	
	private String tableName; // topic_reputation
	private Map<String, Integer> hash_keyword_count = new HashMap<String, Integer>();
	
	@Override
	public String processRequest(HttpServletRequest request) {
		this.requestAndTrimParams(request);
		JSONObject errorResponse = this.validateParams();
		if (errorResponse != null) {
			return errorResponse.toString();
		}
		List<String> postIdList = this.queryPostIdList();
		List<ArticleModel.Article> articleList = this.queryArticleList(postIdList);
		if (articleList != null) {
			for (ArticleModel.Article article : articleList) {
				String title = StringUtils.trimToEmpty(article.getTitle());
				String content = StringUtils.trimToEmpty(article.getContent());
				String longSentance = title + "\n" + content;
				
				List<SegToken> tokens = MyJiebaSegmenter.getInstance().process(longSentance);
				for (SegToken segToken : tokens) {
					String word = StringUtils.trimToEmpty(segToken.word);
					if (word.length() >= 2) {
						if (!this.hash_keyword_count.containsKey(word)) {
							this.hash_keyword_count.put(word, 0);
						}
						int count = this.hash_keyword_count.get(word) + 1;
						this.hash_keyword_count.put(word, count);
					}
				}
			}
			
			JSONArray resultArray = new JSONArray();
			Iterator<String> iterator = this.hash_keyword_count.keySet().iterator();
			while (iterator.hasNext()) {
				String keyword = iterator.next();
				int count = this.hash_keyword_count.get(keyword);
				JSONObject jsonObj = new JSONObject();
				jsonObj.put("keyword", keyword);
				jsonObj.put("count", count);
				resultArray.put(jsonObj);
			}
			JSONArray sortedResultArray = this.getSortedResultArray(resultArray, this.limit);
			JSONObject successObject = ApiResponse.successTemplate();
			successObject.put("result", sortedResultArray);
			return successObject.toString();
		}
		
		return ApiResponse.unknownError().toString();
	}
	
	private JSONArray getSortedResultArray(JSONArray resultArray, int limit) {
		if (resultArray.length() == 0) {
			return resultArray;
		}
		JSONArray sortedJsonArray = new JSONArray();
		
		// Step_1: parse your array in a list
		List<JSONObject> jsonList = new ArrayList<JSONObject>();
		for (int i = 0; i < resultArray.length(); i++) {
		    jsonList.add(resultArray.getJSONObject(i));
		}
		
		// Step_2: then use collection.sort to sort the newly created list
		Collections.sort(jsonList, new Comparator<JSONObject>() {
		    public int compare(JSONObject a, JSONObject b) {
		    	Integer valA = 0;
		    	Integer valB = 0;
		        try {
		            valA = (Integer) a.get("count");
		            valB = (Integer) b.get("count");
		        } 
		        catch (Exception e) {
		            e.printStackTrace();
		        }
		        return valA.compareTo(valB) * -1;
		    }
		});
		
		// Handle for limit: 
		int listSize = jsonList.size();
		int recordSize = limit;
		if (limit > listSize) {
			recordSize = listSize;
		}
		jsonList = jsonList.subList(0, recordSize);
		
		// Step_3: Insert the sorted values in your array
		for (int i = 0; i < jsonList.size(); i++) {
			JSONObject jsonObject = jsonList.get(i);
			sortedJsonArray.put(jsonObject);
		}
		
		return sortedJsonArray;
	}

	private String genQueryPostIdSQL() {
		StringBuffer selectSQL = new StringBuffer();
		selectSQL.append("SELECT id AS post_id FROM ").append(this.tableName);
		selectSQL.append(" WHERE user = ? AND project_name = ? ");
		if (!StringUtils.isEmpty(topic)) {
			selectSQL.append("AND topic IN (");
			for (int i = 0; i < topicValueArr.length; i++) {
				if (i == 0) selectSQL.append("?");
				else selectSQL.append(",?");
			}
			selectSQL.append(") ");
		}
		
		if (!StringUtils.isEmpty(source)) {
			selectSQL.append("AND source IN (");
			for (int i = 0; i < sourceValueArr.length; i++) {
				if (i == 0) selectSQL.append("?");
				else selectSQL.append(",?");
			}
			selectSQL.append(") ");
		}
		if (!StringUtils.isEmpty(website)) {
			selectSQL.append("AND website_name IN (");
			for (int i = 0; i < websiteNameValueArr.length; i++) {
				if (i == 0) selectSQL.append("?");
				else selectSQL.append(",?");
			}
			selectSQL.append(") ");
		}
		if (!StringUtils.isEmpty(channel)) {
			selectSQL.append("AND channel_id IN (");
			for (int i = 0; i < channelIdValueArr.length; i++) {
				if (i == 0) selectSQL.append("?");
				else selectSQL.append(",?");
			}
			selectSQL.append(") ");
		}
		if (!StringUtils.isEmpty(sentiment)) { 
			selectSQL.append("AND sentiment IN (");
			for (int i = 0; i < sentimentValueArr.length; i++) {
				if (i == 0) selectSQL.append("?");
				else selectSQL.append(",?");
			}
			selectSQL.append(") ");
		}
		selectSQL.append("AND DATE_FORMAT(rep_date, '%Y-%m-%d') >= ? ");
		selectSQL.append("AND DATE_FORMAT(rep_date, '%Y-%m-%d') <= ? ");
		// selectSQL.append("ORDER BY date DESC");
		return selectSQL.toString();
	}
	
	private void setWhereClauseValues(PreparedStatement preparedStatement) throws Exception {
		int idx = 0;
		preparedStatement.setObject(++idx, this.user);
		preparedStatement.setObject(++idx, this.project_name);
		
		if (!StringUtils.isEmpty(topic)) {
			for (String topicValue : topicValueArr) {
				int parameterIndex = idx + 1;
				preparedStatement.setObject(parameterIndex, topicValue);
				idx++;
			}
		}
		
		if (!StringUtils.isEmpty(source)) {
			for (String sourceValue : sourceValueArr) {
				int parameterIndex = idx + 1;
				preparedStatement.setObject(parameterIndex, sourceValue);
				idx++;
			}
		}
		if (!StringUtils.isEmpty(website)) {
			for (String websiteName : websiteNameValueArr) {
				int parameterIndex = idx + 1;
				preparedStatement.setObject(parameterIndex, websiteName);
				idx++;
			}
		}
		if (!StringUtils.isEmpty(channel)) {
			for (String channelId : channelIdValueArr) {
				int parameterIndex = idx + 1;
				preparedStatement.setObject(parameterIndex, channelId);
				idx++;
			}
		}
		if (!StringUtils.isEmpty(sentiment)) {
			for (String sentimentValue : sentimentValueArr) {
				int parameterIndex = idx + 1;
				preparedStatement.setObject(parameterIndex, sentimentValue);
				idx++;
			}
		}
		
		int startDateIndex = idx + 1;
		preparedStatement.setObject(startDateIndex, this.startDate);
		idx++;
		
		int endDateIndex = idx + 1;
		preparedStatement.setObject(endDateIndex, this.endDate);
		idx++;
	}

	private List<String> queryPostIdList() {
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		List<String> postIdList = new ArrayList<>();
		try {
			conn = DBUtil.getConn();
			preparedStatement = conn.prepareStatement(this.genQueryPostIdSQL());
			this.setWhereClauseValues(preparedStatement);
			LOGGER.debug("ps_queryPostIdSQL = " + preparedStatement.toString());
			
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				String post_id = rs.getString("post_id");
				postIdList.add(post_id);
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, preparedStatement, conn);
		}
		return postIdList;
	}

	private void requestAndTrimParams(HttpServletRequest request) {
		this.user = StringUtils.trimToEmpty(request.getParameter("user"));
		this.project_name = StringUtils.trimToEmpty(request.getParameter("project_name"));
		
		this.topic = StringUtils.trimToEmpty(request.getParameter("topic")); 
		this.source = StringUtils.trimToEmpty(request.getParameter("source")); 
		this.website = StringUtils.trimToEmpty(request.getParameter("website")); 
		this.channel = StringUtils.trimToEmpty(request.getParameter("channel")); 
		this.sentiment = StringUtils.trimToEmpty(request.getParameter("sentiment"));
		
		this.startDate = StringUtils.trimToEmpty(request.getParameter("start_date"));
		this.endDate = StringUtils.trimToEmpty(request.getParameter("end_date"));
		
		String limitStr = StringUtils.trimToEmpty(request.getParameter("limit"));
		if (!StringUtils.isEmpty(limitStr)) {
			try {
				this.limit = Integer.parseInt(limitStr);
			} catch (Exception e) {
				LOGGER.error(e.getMessage());
			}
		}
		
		this.tableName = TABLE_TOPIC_REPUTATION;
		
		// 多個參數值: 
		this.topicValueArr = this.topic.split(PARAM_VALUES_SEPARATOR);
		this.sourceValueArr = this.source.split(PARAM_VALUES_SEPARATOR);
		this.websiteNameValueArr = this.website.split(PARAM_VALUES_SEPARATOR); // website names
		this.channelIdValueArr = this.channel.split(PARAM_VALUES_SEPARATOR); // channel ids
		this.sentimentValueArr = this.sentiment.split(PARAM_VALUES_SEPARATOR);
	}
	
	private JSONObject validateParams() {
		if (StringUtils.isBlank(this.user) || StringUtils.isBlank(this.project_name)
				|| StringUtils.isBlank(this.startDate) || StringUtils.isBlank(this.endDate)) {
			
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}
		
		if (StringUtils.isBlank(this.topic) && StringUtils.isBlank(this.source)  
				&& StringUtils.isBlank(this.website) && StringUtils.isBlank(this.channel) 
				&& StringUtils.isBlank(this.sentiment)) {
			
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}
		
		if (StringUtils.isBlank(this.startDate) || StringUtils.isBlank(this.endDate)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}
		
		if (!Common.isValidDate(this.startDate, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid start_date.");
		}
		this.startDate = Common.formatDate(this.startDate, "yyyy-MM-dd");
		
		if (!Common.isValidDate(this.endDate, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid end_date.");
		}
		this.endDate = Common.formatDate(this.endDate, "yyyy-MM-dd");
		
		if (!Common.isValidStartDate(this.startDate, this.endDate, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid period values.");
		}
		
//		if (this.pageNum < 1) {
//			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid page_num.");
//		}
//		if (this.pageSize < 1) {
//			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid page_size.");
//		}
		
		return null;
	}

}
