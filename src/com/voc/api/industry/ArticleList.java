package com.voc.api.industry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
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
 * 查詢話題列表
 * 
 * Method	URL
 * GET	/industry/article-list.jsp
 * 
 * Type	Name	Values	Description
 * query	brand		string	欲查詢品牌
 * query	series		string	欲查詢系列
 * query	product		string	欲查詢產品
 * query	source		string	欲查詢來源類型 (facebook、forum、news、ptt)
 * query	website		string	欲查詢來源網站
 * query	channel		string	欲查詢來源頻道
 * query	features	string	欲查詢特性
 * query	*start_date	string	欲查詢起始日期
 * query	*end_date	string	欲查詢結束日期
 * query	page_num	string	頁數 Default: 1
 * query	page_size	string	筆數 Default: 10
 * query	*api_key	string	MORE API token
 * 
 * Step_1: 取得文章id(post_id) List: (In brand_reputation OR product_reputation): 
 * 	SELECT id AS post_id FROM ibuzz_voc.product_reputation 
 * 	WHERE brand = 'BENZ' AND series = '掀背車' AND product = 'B180'
 * 	AND website_id in ('5b29c821a85d0a7df5c3ff22', '5b29c824a85d0a7df5c40080')
 * 	AND channel_id in ('5ad450dea85d0a2afac34a9b', '5ad450dea85d0a2afac34aa2')
 * 	AND DATE_FORMAT(date, '%Y-%m-%d') >= '2018-01-01' AND DATE_FORMAT(date, '%Y-%m-%d') <= '2018-06-30'
 *  -- ORDER BY date DESC;
 *  
 * Step_2: 取得文章列表(post_list)資訊(分頁): 
 * 	SELECT id, url, title, content, author, DATE_FORMAT(date, '%Y-%m-%d %H:%i:%s') AS date, website_name, channel_name, comment_count 
 * 	FROM ibuzz_voc.post_list 
 * 	WHERE id in ('5b1f7e3aa85d0a0145294293','5b1f7e3aa85d0a0145294294') 
 *  ORDER BY date DESC
 * 	LIMIT 0, 10;
 * 
 * Note:
 * LIMIT a, b
 * a = (page_num - 1) * page_size
 * b = page_size
 * 
 * 
 * EX: API URL:
 * http://localhost:8080/voc/industry/article-list.jsp?brand=BENZ&series=掀背車&product=B180&website=5b29c821a85d0a7df5c3ff22;5b29c824a85d0a7df5c40080&channel=5ad450dea85d0a2afac34a9b;5ad450dea85d0a2afac34aa2&start_date=2018-01-01&end_date=2018-06-30&page_num=1&page_size=10
 *  
 */
public class ArticleList extends RootAPI {
	private static final Logger LOGGER = LoggerFactory.getLogger(ArticleList.class);
	private static final Gson GSON = new Gson();
	private Map<String, String[]> parameterMap;
	private String brand;   // 單個參數值
	private String series;  // 單個參數值
	private String product; // 單個參數值
	private String source;  // 多個參數值
	private String website; // 多個參數值 (website id)
	private String channel; // 多個參數值 (channel id)
	private String features;
	private String startDate; // start_date
	private String endDate; // end_date
	private int pageNum = 1; // page_num: 頁數 Default: 1
	private int pageSize = 10; // page_size: 筆數 Default: 10

	// 多個參數值
	private String[] sourceValueArr;
	private String[] websiteIdValueArr;
	private String[] channelIdValueArr;
	
	private String tableName; // brand_reputation OR product_reputation
	
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
			ArticleListModel articleListModel = new ArticleListModel();
			articleListModel.setSuccess(true);
			articleListModel.setTotal(postIdList.size());
			articleListModel.setPage_num(this.pageNum);
			articleListModel.setPage_size(this.pageSize);
			articleListModel.setResult(articleList);
			String responseJsonStr = GSON.toJson(articleListModel);
			LOGGER.info("responseJsonStr=" + responseJsonStr);
			return responseJsonStr;
		}
		return ApiResponse.unknownError().toString();
	}
	
	private List<ArticleModel.Article> queryArticleList(List<String> postIdList) {
		List<ArticleModel.Article> articleList = new ArrayList<>();
		if (postIdList == null || postIdList.size() == 0) {
			return articleList;
		}
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		try {
			StringBuffer queryArticleSQL = new StringBuffer();
			queryArticleSQL.append("SELECT id, url, title, author, DATE_FORMAT(date, '%Y-%m-%d %H:%i:%s') AS date, website_name, channel_name, comment_count ");
			queryArticleSQL.append("FROM ").append(TABLE_POST_LIST).append(" ");
			queryArticleSQL.append("WHERE id in (");
			for(int i = 0 ; i < postIdList.size(); i++ ) {
				if (i == 0) queryArticleSQL.append("?");
				else queryArticleSQL.append(",?");
			}
			queryArticleSQL.append(") ");
			queryArticleSQL.append("ORDER BY date DESC ");
			queryArticleSQL.append("LIMIT ?, ?");
			
			conn = DBUtil.getConn();
			preparedStatement = conn.prepareStatement(queryArticleSQL.toString());
			int i = 0;
			for (String postId : postIdList) {
				int parameterIndex = i+1;
				preparedStatement.setObject(parameterIndex, postId);
				i++;
			}
			
			int pageNumIndex = i + 1;
			preparedStatement.setInt(pageNumIndex, ((this.pageNum - 1) * this.pageSize));
			i++;
			
			int pageSizeIndex = i + 1;
			preparedStatement.setInt(pageSizeIndex, (this.pageSize));
			i++;
			
			LOGGER.debug("ps_queryArticleSQL = " + preparedStatement.toString());

			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				ArticleModel.Article article = new ArticleModel().new Article();
				article.setPost_id(rs.getString("id"));
				article.setUrl(rs.getString("url"));
				article.setTitle(rs.getString("title"));
				article.setAuthor(rs.getString("author"));
				article.setDate(rs.getString("date"));
				article.setChannel(rs.getString("website_name") + "_" + rs.getString("channel_name"));
				article.setComment_count(rs.getInt("comment_count"));
				articleList.add(article);
			}
			return articleList;
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, preparedStatement, conn);
		}
		return null;
	}

	private String genQueryPostIdSQL() {
		int conditionCnt = 0;
		StringBuffer selectSQL = new StringBuffer();
		selectSQL.append("SELECT id AS post_id FROM ").append(this.tableName);
		selectSQL.append(" WHERE ");
		if (!StringUtils.isEmpty(brand)) {
			selectSQL.append("brand = ? ");
			conditionCnt++;
		}
		if (!StringUtils.isEmpty(series)) {
			if (conditionCnt > 0) {
				selectSQL.append("AND ");
			}
			selectSQL.append("series = ? ");
			conditionCnt++;
		}
		if (!StringUtils.isEmpty(product)) {
			if (conditionCnt > 0) {
				selectSQL.append("AND ");
			}
			selectSQL.append("product = ? ");
			conditionCnt++;
		}
		
		if (!StringUtils.isEmpty(source)) {
			if (conditionCnt > 0) {
				selectSQL.append("AND ");
			}
			selectSQL.append("source IN (");
			for (int i = 0; i < sourceValueArr.length; i++) {
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
			selectSQL.append("website_id IN (");
			for (int i = 0; i < websiteIdValueArr.length; i++) {
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
			for (int i = 0; i < channelIdValueArr.length; i++) {
				if (i == 0) selectSQL.append("?");
				else selectSQL.append(",?");
			}
			selectSQL.append(") ");
			conditionCnt++;
		}
		if (!StringUtils.isEmpty(features)) {
			// TODO: Do something later... 
		}
		selectSQL.append("AND DATE_FORMAT(date, '%Y-%m-%d') >= ? ");
		selectSQL.append("AND DATE_FORMAT(date, '%Y-%m-%d') <= ? ");
		// selectSQL.append("ORDER BY date DESC");
		return selectSQL.toString();
	}
	
	private void setWhereClauseValues(PreparedStatement preparedStatement) throws Exception {
		int idx = 0;
		if (!StringUtils.isEmpty(brand)) {
			int parameterIndex = idx + 1;
			preparedStatement.setObject(parameterIndex, brand);
			idx++;
		}
		if (!StringUtils.isEmpty(series)) {
			int parameterIndex = idx + 1;
			preparedStatement.setObject(parameterIndex, series);
			idx++;
		}
		if (!StringUtils.isEmpty(product)) {
			int parameterIndex = idx + 1;
			preparedStatement.setObject(parameterIndex, product);
			idx++;
		}
		
		if (!StringUtils.isEmpty(source)) {
			for (String sourceValue : sourceValueArr) {
				int parameterIndex = idx + 1;
				preparedStatement.setObject(parameterIndex, sourceValue);
				idx++;
			}
		}
		if (!StringUtils.isEmpty(website)) {
			for (String websiteId : websiteIdValueArr) {
				int parameterIndex = idx + 1;
				preparedStatement.setObject(parameterIndex, websiteId);
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
		if (!StringUtils.isEmpty(features)) {
			// TODO: Do something later... 
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
		this.parameterMap = request.getParameterMap();
		this.brand = StringUtils.trimToEmpty(request.getParameter("brand"));       // 單個參數值
		this.series = StringUtils.trimToEmpty(request.getParameter("series"));     // 單個參數值
		this.product = StringUtils.trimToEmpty(request.getParameter("product"));   // 單個參數值
		this.source = StringUtils.trimToEmpty(request.getParameter("source"));     // 多個參數值
		this.website = StringUtils.trimToEmpty(request.getParameter("website"));   // 多個參數值 (website id)
		this.channel = StringUtils.trimToEmpty(request.getParameter("channel"));   // 多個參數值 (channel id)
		this.features = StringUtils.trimToEmpty(request.getParameter("features"));
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
		
		this.tableName = this.getTableName(this.parameterMap);
		
		// 多個參數值: 
		this.sourceValueArr = this.source.split(PARAM_VALUES_SEPARATOR);
		this.websiteIdValueArr = this.website.split(PARAM_VALUES_SEPARATOR); // website ids
		this.channelIdValueArr = this.channel.split(PARAM_VALUES_SEPARATOR); // channel ids
	}
	
	private JSONObject validateParams() {
		if (StringUtils.isBlank(this.brand) && StringUtils.isBlank(this.series) && StringUtils.isBlank(this.product) 
				&& StringUtils.isBlank(this.source) && StringUtils.isBlank(this.website) && StringUtils.isBlank(this.channel) 
				&& StringUtils.isBlank(this.features)) {
			
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
