package com.voc.api.industry;

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
 * 查詢特性詞文章列表: 查詢時間區間內觀測的特性詞文章列表。
 * 
 * 
 * 
 * Step_1: 取得文章id(post_id) List: (in feature_reputation): 
 * 	SELECT DISTINCT id AS post_id FROM ibuzz_voc.feature_reputation 
 * 	WHERE industry IN ('汽車產業') AND features IN ('諮詢', '期待') AND website_name IN ('NowNews','PTT') AND channel_id IN ('5ad450d9a85d0a2afac3466d','5ad450dfa85d0a2afac34b6d') 
 * 	AND DATE_FORMAT(date, '%Y-%m-%d') >= '2018-01-01' AND DATE_FORMAT(date, '%Y-%m-%d') <= '2018-06-30';
 * 
 *  SELECT DISTINCT id AS post_id FROM ibuzz_voc.feature_reputation 
 *  WHERE industry IN ('汽車','單車') AND features IN ('E-NCAP', 'ADS','佔空間') AND sentiment IN ('1','0','-1') 
 *  AND DATE_FORMAT(date, '%Y-%m-%d') >= '2019-01-01' AND DATE_FORMAT(date, '%Y-%m-%d') <= '2019-12-31';
 *  
 * 
 * Step_2: 取得文章列表(post_list)資訊(分頁): 
 * 	SELECT id, url, title, author, DATE_FORMAT(date, '%Y-%m-%d %H:%i:%s') AS date, website_name, channel_name, comment_count 
 * 	FROM ibuzz_voc.post_list 
 * 	WHERE id in ('5b713747a85d0a4e326691d6','5b713748a85d0a4e3266923d') 
 * 	ORDER BY date DESC
 * 	LIMIT 0, 10;
 * 
 * EX: API URL:
 * http://localhost:8080/voc/industry/feature-article-list.jsp?industry=汽車產業&features=諮詢;期待&website=NowNews;PTT&channel=5ad450d9a85d0a2afac3466d;5ad450dfa85d0a2afac34b6d&start_date=2018-01-01&end_date=2018-06-30&page_num=1&page_size=10
 * 
 * 
 * Requirement Change: 
 * 1.加上 sentiment(評價): 1:偏正、0:中性、-1:偏負
 * 
 * EX: 
 * http://localhost:8080/voc/industry/feature-article-list.jsp?industry=汽車產業&features=E-NCAP;ADS;佔空間&sentiment=1;0;-1&start_date=2019-01-01&end_date=2019-12-31&page_num=1&page_size=10
 *
 */
public class FeatureArticleList extends RootAPI {
	private static final Logger LOGGER = LoggerFactory.getLogger(FeatureArticleList.class);
	private static final Gson GSON = new Gson();
	
	private String industry; // 必填: 
	private String brand;
	private String series;
	private String product;
	private String mediaType; // 欲查詢來源類型(facebook、forum、news、ptt)
	private String website; // website_name
	private String channel; // channel id
	private String features; // 必填: 
	private String sentiment; // 欲查詢評價 (1:偏正、0:中性、-1:偏負)
	
	private String startDate; // 必填: start_date
	private String endDate; // 必填: end_date
	private int pageNum = 1; // page_num: 頁數 Default: 1
	private int pageSize = 10; // page_size: 筆數 Default: 10
	
	private String[] industryValueArr;
	private String[] brandValueArr;
	private String[] seriesValueArr;
	private String[] productValueArr;
	private String[] mediaTypeValueArr; 
	private String[] websiteValueArr;
	private String[] channelValueArr;
	private String[] featuresValueArr;
	private String[] sentimentValueArr;
	
	private Map<String, Integer> hash_postId_reputation = new HashMap<String, Integer>();
	
	@Override
	public String processRequest(HttpServletRequest request) {
		this.requestAndTrimParams(request);
		JSONObject errorResponse = this.validateParams();
		if (errorResponse != null) {
			return errorResponse.toString();
		}
		List<String> postIdList = this.queryPostIdList();
		List<ArticleModel.Article> articleList = this.queryArticleList(postIdList, this.hash_postId_reputation, this.pageNum, this.pageSize);
		if (articleList != null) {
			ArticleListModel articleListModel = new ArticleListModel();
			articleListModel.setSuccess(true);
			articleListModel.setTotal(postIdList.size());
			articleListModel.setPage_num(this.pageNum);
			articleListModel.setPage_size(this.pageSize);
			articleListModel.setResult(articleList);
			String responseJsonStr = GSON.toJson(articleListModel);
//			LOGGER.info("responseJsonStr=" + responseJsonStr);
			return responseJsonStr;
		}
		return ApiResponse.unknownError().toString();
	}
	
	private void requestAndTrimParams(HttpServletRequest request) {
		this.industry = StringUtils.trimToEmpty(request.getParameter("industry"));
		this.brand = StringUtils.trimToEmpty(request.getParameter("brand")); 
		this.series = StringUtils.trimToEmpty(request.getParameter("series")); 
		this.product = StringUtils.trimToEmpty(request.getParameter("product")); 
		this.mediaType = StringUtils.trimToEmpty(request.getParameter("media_type")); 
		this.website = StringUtils.trimToEmpty(request.getParameter("website")); 
		this.channel = StringUtils.trimToEmpty(request.getParameter("channel")); 
		this.features = StringUtils.trimToEmpty(request.getParameter("features"));
		this.sentiment = StringUtils.trimToEmpty(request.getParameter("sentiment")); 
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
		
		// 多個參數值: 
		this.industryValueArr = this.industry.split(PARAM_VALUES_SEPARATOR);
		this.brandValueArr = this.brand.split(PARAM_VALUES_SEPARATOR);
		this.seriesValueArr = this.series.split(PARAM_VALUES_SEPARATOR);
		this.productValueArr = this.product.split(PARAM_VALUES_SEPARATOR);
		this.mediaTypeValueArr = this.mediaType.split(PARAM_VALUES_SEPARATOR);
		this.websiteValueArr = this.website.split(PARAM_VALUES_SEPARATOR); // website names
		this.channelValueArr = this.channel.split(PARAM_VALUES_SEPARATOR); // channel ids
		this.featuresValueArr = this.features.split(PARAM_VALUES_SEPARATOR);
		this.sentimentValueArr = this.sentiment.split(PARAM_VALUES_SEPARATOR);
	}
	
	private JSONObject validateParams() {
		if (StringUtils.isBlank(this.industry) || StringUtils.isBlank(this.features)) {
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
	
	private String genQueryPostIdSQL() {
		int conditionCnt = 0;
		StringBuffer selectSQL = new StringBuffer();
		selectSQL.append("SELECT DISTINCT id AS post_id, SUM(reputation) as reputation FROM ").append(TABLE_FEATURE_REPUTATION);
		selectSQL.append(" WHERE ");
		if (!StringUtils.isEmpty(industry)) {
			selectSQL.append("industry IN (");
			for (int i = 0; i < industryValueArr.length; i++) {
				if (i == 0) selectSQL.append("?");
				else selectSQL.append(",?");
			}
			selectSQL.append(") ");
			conditionCnt++;
		}
		if (!StringUtils.isEmpty(brand)) {
			if (conditionCnt > 0) {
				selectSQL.append("AND ");
			}
			if (StringUtils.isEmpty(series) && StringUtils.isEmpty(product)) {
				selectSQL.append("series = '' AND product = '' AND ");
			}
			selectSQL.append("brand IN (");
			for (int i = 0; i < brandValueArr.length; i++) {
				if (i == 0) selectSQL.append("?");
				else selectSQL.append(",?");
			}
			selectSQL.append(") ");
			conditionCnt++;
		}
		if (!StringUtils.isEmpty(series)) {
			if (conditionCnt > 0) {
				selectSQL.append("AND ");
			}
			selectSQL.append("series IN (");
			for (int i = 0; i < seriesValueArr.length; i++) {
				if (i == 0) selectSQL.append("?");
				else selectSQL.append(",?");
			}
			selectSQL.append(") ");
			conditionCnt++;
		}
		if (!StringUtils.isEmpty(product)) {
			if (conditionCnt > 0) {
				selectSQL.append("AND ");
			}
			selectSQL.append("product IN (");
			for (int i = 0; i < productValueArr.length; i++) {
				if (i == 0) selectSQL.append("?");
				else selectSQL.append(",?");
			}
			selectSQL.append(") ");
			conditionCnt++;
		}
		
		if (!StringUtils.isEmpty(mediaType)) {
			if (conditionCnt > 0) {
				selectSQL.append("AND ");
			}
			selectSQL.append("media_type IN (");
			for (int i = 0; i < mediaTypeValueArr.length; i++) {
				if (i == 0) selectSQL.append("?");
				else selectSQL.append(",?");
			}
			selectSQL.append(") ");
			conditionCnt++;
		}
		
		if (!StringUtils.isEmpty(website)) {
			if (conditionCnt > 0) {
				selectSQL.append("AND ");
			}
			selectSQL.append("website_name IN (");
			for (int i = 0; i < websiteValueArr.length; i++) {
				if (i == 0) selectSQL.append("?");
				else selectSQL.append(",?");
			}
			selectSQL.append(") ");
			conditionCnt++;
		}
		if (!StringUtils.isEmpty(channel)) {
			if (conditionCnt > 0) {
				selectSQL.append("AND ");
			}
			selectSQL.append("channel_id IN (");
			for (int i = 0; i < channelValueArr.length; i++) {
				if (i == 0) selectSQL.append("?");
				else selectSQL.append(",?");
			}
			selectSQL.append(") ");
			conditionCnt++;
		}
		if (!StringUtils.isEmpty(features)) {
			if (conditionCnt > 0) {
				selectSQL.append("AND ");
			}
			selectSQL.append("features IN (");
			for (int i = 0; i < featuresValueArr.length; i++) {
				if (i == 0) selectSQL.append("?");
				else selectSQL.append(",?");
			}
			selectSQL.append(") ");
			conditionCnt++;
		}
		if (!StringUtils.isEmpty(sentiment)) {
			if (conditionCnt > 0) {
				selectSQL.append("AND ");
			}
			selectSQL.append("sentiment IN (");
			for (int i = 0; i < sentimentValueArr.length; i++) {
				if (i == 0) selectSQL.append("?");
				else selectSQL.append(",?");
			}
			selectSQL.append(") ");
			conditionCnt++;
		}
		selectSQL.append("AND DATE_FORMAT(rep_date, '%Y-%m-%d') >= ? ");
		selectSQL.append("AND DATE_FORMAT(rep_date, '%Y-%m-%d') <= ? ");
		selectSQL.append("GROUP BY post_id");
		return selectSQL.toString();
	}
	
	private void setWhereClauseValues(PreparedStatement preparedStatement) throws Exception {
		int idx = 0;
		if (!StringUtils.isEmpty(industry)) {
			for (String industryValue : industryValueArr) {
				int parameterIndex = idx + 1;
				preparedStatement.setObject(parameterIndex, industryValue);
				idx++;
			}
		}
		if (!StringUtils.isEmpty(brand)) {
			for (String brandValue : brandValueArr) {
				int parameterIndex = idx + 1;
				preparedStatement.setObject(parameterIndex, brandValue);
				idx++;
			}
		}
		if (!StringUtils.isEmpty(series)) {
			for (String seriesValue : seriesValueArr) {
				int parameterIndex = idx + 1;
				preparedStatement.setObject(parameterIndex, seriesValue);
				idx++;
			}
		}
		if (!StringUtils.isEmpty(product)) {
			for (String productValue : productValueArr) {
				int parameterIndex = idx + 1;
				preparedStatement.setObject(parameterIndex, productValue);
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
			for (String websiteName : websiteValueArr) {
				int parameterIndex = idx + 1;
				preparedStatement.setObject(parameterIndex, websiteName);
				idx++;
			}
		}
		if (!StringUtils.isEmpty(channel)) {
			for (String channelId : channelValueArr) {
				int parameterIndex = idx + 1;
				preparedStatement.setObject(parameterIndex, channelId);
				idx++;
			}
		}
		if (!StringUtils.isEmpty(features)) {
			for (String features : featuresValueArr) {
				int parameterIndex = idx + 1;
				preparedStatement.setObject(parameterIndex, features);
				idx++;
			}
		}
		if (!StringUtils.isEmpty(sentiment)) {
			for (String sentiment : sentimentValueArr) {
				int parameterIndex = idx + 1;
				preparedStatement.setObject(parameterIndex, sentiment);
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

}
