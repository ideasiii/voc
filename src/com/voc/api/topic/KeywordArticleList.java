package com.voc.api.topic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.voc.api.RootAPI;
import com.voc.api.model.ArticleListModel;
import com.voc.api.model.ArticleModel;
import com.voc.common.ApiResponse;
import com.voc.common.Common;
import com.voc.common.DBUtil;

/**
 * 查詢自訂主題關鍵詞文章列表: 查詢時間區間內關鍵詞文章列表
 * /topic/keyword-article-list.jsp
 * 
 * EX-1:
 * - URL:
 *   ==>http://localhost:8080/voc/topic/keyword-article-list.jsp?user=tsmc&project_name=自訂汽車專案&topic=日系汽車;歐系汽車&website=PTT;Mobile01&keyword=挑戰&start_date=2018-12-01&end_date=2018-12-31&page_num=1&page_size=3
 *   ==>http://localhost:8080/voc/topic/keyword-article-list.jsp?user=5bff9b81617d854c850a0d15&project_name=自訂汽車專案&website=PTT;Mobile01&keyword=挑戰&start_date=2019-01-01&end_date=2019-12-31&page_num=1&page_size=3
 *   ==>http://localhost:8080/voc/topic/keyword-article-list.jsp?user=5bff9b81617d854c850a0d15&project_name=自訂汽車專案&website=PTT;Mobile01&sentiment=1;0;-1&keyword=挑戰&start_date=2019-01-01&end_date=2019-12-31&page_num=1&page_size=3
 * 
 * SQL Example:
 *   SELECT id AS post_id FROM ibuzz_voc.topic_reputation 
 *   WHERE user = '5bff9b81617d854c850a0d15' AND project_name = '自訂汽車專案' AND website_name IN ('PTT','Mobile01') 
 *   AND sentiment IN ('1','0','-1') 
 *   AND DATE_FORMAT(date, '%Y-%m-%d') >= '2019-01-01' AND DATE_FORMAT(date, '%Y-%m-%d') <= '2019-12-31'
 *   
 *   
 */
public class KeywordArticleList extends RootAPI {
	private static final Logger LOGGER = LoggerFactory.getLogger(KeywordArticleList.class);
	private static final Gson GSON = new Gson();
	
	private String user; // 欲查詢產業別
	private String project_name; // 欲查詢品牌
	
	private String topic;
	private String mediaType; //來源類型
	private String website;
	private String channel;
	private String sentiment;
	
	private String keyword; // 關鍵詞: 用程式處理
	
	private String startDate; // start_date
	private String endDate; // end_date
	private int pageNum = 1; // page_num: 頁數 Default: 1
	private int pageSize = 10; // page_size: 筆數 Default: 10

	// 多個參數值
	private String[] topicValueArr;
	private String[] mediaTypeValueArr;
	private String[] websiteNameValueArr;
	private String[] channelIdValueArr;
	private String[] sentimentValueArr;
	
	private String tableName; // topic_reputation
	private Map<String, Integer> hash_postId_reputation = new HashMap<String, Integer>();

	@Override
	public String processRequest(HttpServletRequest request) {
		this.requestAndTrimParams(request);
		JSONObject errorResponse = this.validateParams();
		if (errorResponse != null) {
			return errorResponse.toString();
		}
		List<String> postIdList = this.queryPostIdList();
		List<ArticleModel.Article> articleList = this.queryArticleList(postIdList, this.hash_postId_reputation, this.keyword);
		
		int toIndex = this.pageSize * this.pageNum;
		int maxIdx = articleList.size() - 1;
		if (toIndex > maxIdx) {
			toIndex = maxIdx;
		}
		List<ArticleModel.Article> pageList = new ArrayList<ArticleModel.Article>();
		if (articleList != null && articleList.size() > 0) {
			pageList = articleList.subList(this.pageSize * (this.pageNum - 1), toIndex);
		}
		
		if (pageList != null) {
			ArticleListModel articleListModel = new ArticleListModel();
			articleListModel.setSuccess(true);
			articleListModel.setTotal(articleList.size()); 
			articleListModel.setPage_num(this.pageNum);
			articleListModel.setPage_size(this.pageSize);
			articleListModel.setResult(pageList);
			String responseJsonStr = GSON.toJson(articleListModel);
//			LOGGER.info("responseJsonStr=" + responseJsonStr);
			return responseJsonStr;
		}
		return ApiResponse.unknownError().toString();
	}

	private String genQueryPostIdSQL() {
		StringBuffer selectSQL = new StringBuffer();
		selectSQL.append("SELECT id AS post_id, SUM(reputation) as reputation FROM ").append(this.tableName);
		selectSQL.append(" WHERE user = ? AND project_name = ? ");
		if (!StringUtils.isEmpty(topic)) {
			selectSQL.append("AND topic IN (");
			for (int i = 0; i < topicValueArr.length; i++) {
				if (i == 0) selectSQL.append("?");
				else selectSQL.append(",?");
			}
			selectSQL.append(") ");
		}
		
		if (!StringUtils.isEmpty(mediaType)) {
			selectSQL.append("AND media_type IN (");
			for (int i = 0; i < mediaTypeValueArr.length; i++) {
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
		selectSQL.append("GROUP BY post_id");
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
		
		if (!StringUtils.isEmpty(mediaType)) {
			for (String mediaTypeValue : mediaTypeValueArr) {
				int parameterIndex = idx + 1;
				preparedStatement.setObject(parameterIndex, mediaTypeValue);
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
				int reputation = rs.getInt("reputation");
				this.hash_postId_reputation.put(post_id, reputation);
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
		this.mediaType = StringUtils.trimToEmpty(request.getParameter("media_type")); 
		this.website = StringUtils.trimToEmpty(request.getParameter("website")); 
		this.channel = StringUtils.trimToEmpty(request.getParameter("channel")); 
		this.sentiment = StringUtils.trimToEmpty(request.getParameter("sentiment"));
		
		this.keyword = StringUtils.trimToEmpty(request.getParameter("keyword"));
		
		this.startDate = StringUtils.trimToEmpty(request.getParameter("start_date"));
		this.endDate = StringUtils.trimToEmpty(request.getParameter("end_date"));

		String pageNumStr = StringUtils.trimToEmpty(request.getParameter("page_num"));
		if (!StringUtils.isEmpty(pageNumStr)) {
			try {
				this.pageNum = Integer.parseInt(pageNumStr);
			} catch (Exception e) {
				LOGGER.error(e.getMessage());
			}
		}
		
		String pageSizeStr = StringUtils.trimToEmpty(request.getParameter("page_size"));
		if (!StringUtils.isEmpty(pageSizeStr)) {
			try {
				this.pageSize = Integer.parseInt(pageSizeStr);
			} catch (Exception e) {
				LOGGER.error(e.getMessage());
			}
		}
		
		this.tableName = TABLE_TOPIC_REPUTATION;
		
		// 多個參數值: 
		this.topicValueArr = this.topic.split(PARAM_VALUES_SEPARATOR);
		this.mediaTypeValueArr = this.mediaType.split(PARAM_VALUES_SEPARATOR);
		this.websiteNameValueArr = this.website.split(PARAM_VALUES_SEPARATOR); // website names
		this.channelIdValueArr = this.channel.split(PARAM_VALUES_SEPARATOR); // channel ids
		this.sentimentValueArr = this.sentiment.split(PARAM_VALUES_SEPARATOR);
	}
	
	private JSONObject validateParams() {
		if (StringUtils.isBlank(this.user) || StringUtils.isBlank(this.project_name)
				|| StringUtils.isBlank(this.startDate) || StringUtils.isBlank(this.endDate)) {
			
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}
		
		if (StringUtils.isBlank(this.topic) && StringUtils.isBlank(this.mediaType)  
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
		
		if (this.pageNum < 1) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid page_num.");
		}
		if (this.pageSize < 1) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid page_size.");
		}
		
		return null;
	}

}
