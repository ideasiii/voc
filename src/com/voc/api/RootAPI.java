package com.voc.api;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voc.api.model.ArticleModel;
import com.voc.common.DBUtil;

public abstract class RootAPI {
	private static final Logger LOGGER = LoggerFactory.getLogger(RootAPI.class);
	public static final String API_KEY = "api_key";
	protected static final String UPDATE_TIME = "update_time";
	protected static final String PARAM_VALUES_SEPARATOR = ";";
	protected static final String TABLE_PRODUCT_REPUTATION = "ibuzz_voc.product_reputation";
	protected static final String TABLE_BRAND_REPUTATION = "ibuzz_voc.brand_reputation";
	protected static final String TABLE_CHANNEL_LIST = "ibuzz_voc.channel_list";
	protected static final String TABLE_WEBSITE_LIST = "ibuzz_voc.website_list";
	protected static final String TABLE_POST_LIST = "ibuzz_voc.post_list";
	protected static final String TABLE_COMMENT_LIST = "ibuzz_voc.comment_list";
	protected static final String TABLE_INDUSTRY_FEATURE_KEYWORD_LIST = "ibuzz_voc.industry_feature_keyword_list";
	protected static final String TABLE_FEATURE_REPUTATION = "ibuzz_voc.feature_reputation";
	protected static final String TABLE_TOPIC_KEYWORD_JOB_LIST = "ibuzz_voc.topic_keyword_job_list";
	protected static final String TABLE_TOPIC_REPUTATION = "ibuzz_voc.topic_reputation";
	private static final Map<String, String> PARAM_COLUMN_MAP = new HashMap<String, String>();
	static {
		PARAM_COLUMN_MAP.put("industry", "industry");
		PARAM_COLUMN_MAP.put("brand", "brand");
		PARAM_COLUMN_MAP.put("series", "series");
		PARAM_COLUMN_MAP.put("product", "product");
		PARAM_COLUMN_MAP.put("source", "source");
		PARAM_COLUMN_MAP.put("website", "website_name");
		PARAM_COLUMN_MAP.put("channel", "channel_id");
		PARAM_COLUMN_MAP.put("features", "features");
		PARAM_COLUMN_MAP.put("start_date", "date");
		PARAM_COLUMN_MAP.put("end_date", "date");
		
		PARAM_COLUMN_MAP.put("user", "user");
		PARAM_COLUMN_MAP.put("project_name", "project_name");
		PARAM_COLUMN_MAP.put("topic", "topic");
		PARAM_COLUMN_MAP.put("sentiment", "sentiment");
	}
//	private static final List<String> VALID_PARAM_NAMES = Arrays.asList("industry", "brand", "series", "product",
//			"source", "website", "channel", "features", "start_date", "end_date", "interval");
	
	private static final List<String> ITEM_PARAM_NAMES = Arrays.asList("industry", "brand", "series", "product",
			"source", "website", "channel", "features", 
			"user", "project_name", "topic", "sentiment");

	public abstract String processRequest(HttpServletRequest request);

	protected String getTableName(Map<String, String[]> parameterMap) {
		if (parameterMap.containsKey("product") || parameterMap.containsKey("series")) {
			return TABLE_PRODUCT_REPUTATION;
		} else {
			return TABLE_BRAND_REPUTATION;
		}
	}
	
	protected String getTableName(List<String> paramNameList) {
		if (paramNameList.contains("product") || paramNameList.contains("series")) {
			return TABLE_PRODUCT_REPUTATION;
		} else {
			return TABLE_BRAND_REPUTATION;
		}
	}

	protected String getColumnName(String paramName) {
		return PARAM_COLUMN_MAP.get(paramName);
	}

//	protected boolean isValidParamName(String paramName) {
//		return VALID_PARAM_NAMES.contains(paramName);
//	}

	protected boolean isItemParamName(String paramName) {
		return ITEM_PARAM_NAMES.contains(paramName);
	}
	
	protected String queryUpdateTime(String selectUpdateTimeSQL) {
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		String update_time = null;
		try {
			conn = DBUtil.getConn();
			preparedStatement = conn.prepareStatement(selectUpdateTimeSQL);
			rs = preparedStatement.executeQuery();
			if (rs.next()) {
				update_time = rs.getString(UPDATE_TIME);
			}
			return StringUtils.trimToEmpty(update_time);
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, preparedStatement, conn);
		}
		return null;
	}
	
	protected String getChannelNameById(String id) {
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		String channelName = null;
		String selectSql = "SELECT name FROM "+ TABLE_CHANNEL_LIST +" WHERE id = ? LIMIT 1";
		try {
			conn = DBUtil.getConn();
			preparedStatement = conn.prepareStatement(selectSql);
			preparedStatement.setString(1, id);
			rs = preparedStatement.executeQuery();
			if (rs.next()) {
				channelName = rs.getString("name");
			}
			return channelName;
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, preparedStatement, conn);
		}
		return null;
	}

	protected String getWebsiteNameById(String id) {
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		String websiteName = null;
		String selectSql = "SELECT name FROM " + TABLE_WEBSITE_LIST + " WHERE id = ? LIMIT 1";
		try {
			conn = DBUtil.getConn();
			preparedStatement = conn.prepareStatement(selectSql);
			preparedStatement.setString(1, id);
			rs = preparedStatement.executeQuery();
			if (rs.next()) {
				websiteName = rs.getString("name");
			}
			return websiteName;
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, preparedStatement, conn);
		}
		return null;
	}
	
//	protected String getChannelNameById(String tableName, String channelId) {
//		Connection conn = null;
//		PreparedStatement preparedStatement = null;
//		ResultSet rs = null;
//		String channelName = null;
//		String selectSql = "SELECT channel_name FROM "+ tableName +" WHERE channel_id = ? LIMIT 1";
//		try {
//			conn = DBUtil.getConn();
//			preparedStatement = conn.prepareStatement(selectSql);
//			preparedStatement.setString(1, channelId);
//			rs = preparedStatement.executeQuery();
//			if (rs.next()) {
//				channelName = rs.getString("channel_name");
//			}
//			return channelName;
//		} catch (Exception e) {
//			LOGGER.error(e.getMessage());
//			e.printStackTrace();
//		} finally {
//			DBUtil.close(rs, preparedStatement, conn);
//		}
//		return null;
//	}

//	protected String getWebsiteNameById(String tableName, String websiteId) {
//		Connection conn = null;
//		PreparedStatement preparedStatement = null;
//		ResultSet rs = null;
//		String websiteName = null;
//		String selectSql = "SELECT website_name FROM " + tableName + " WHERE website_id = ? LIMIT 1";
//		try {
//			conn = DBUtil.getConn();
//			preparedStatement = conn.prepareStatement(selectSql);
//			preparedStatement.setString(1, websiteId);
//			rs = preparedStatement.executeQuery();
//			if (rs.next()) {
//				websiteName = rs.getString("website_name");
//			}
//			return websiteName;
//		} catch (Exception e) {
//			LOGGER.error(e.getMessage());
//			e.printStackTrace();
//		} finally {
//			DBUtil.close(rs, preparedStatement, conn);
//		}
//		return null;
//	}
	
	protected List<ArticleModel.Article> queryArticleList(List<String> postIdList, int pageNum, int pageSize) {
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
			preparedStatement.setInt(pageNumIndex, ((pageNum - 1) * pageSize));
			i++;
			
			int pageSizeIndex = i + 1;
			preparedStatement.setInt(pageSizeIndex, (pageSize));
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
	
	protected List<ArticleModel.Article> queryArticleList(List<String> postIdList) {
		List<ArticleModel.Article> articleList = new ArrayList<>();
		if (postIdList == null || postIdList.size() == 0) {
			return articleList;
		}
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		try {
			StringBuffer queryArticleSQL = new StringBuffer();
			queryArticleSQL.append("SELECT id, url, title, content, author, DATE_FORMAT(date, '%Y-%m-%d %H:%i:%s') AS date, website_name, channel_name, comment_count ");
			queryArticleSQL.append("FROM ").append(TABLE_POST_LIST).append(" ");
			queryArticleSQL.append("WHERE id in (");
			for(int i = 0 ; i < postIdList.size(); i++ ) {
				if (i == 0) queryArticleSQL.append("?");
				else queryArticleSQL.append(",?");
			}
			queryArticleSQL.append(") ");
			queryArticleSQL.append("ORDER BY date DESC ");
//			queryArticleSQL.append("LIMIT ?, ?");
			
			conn = DBUtil.getConn();
			preparedStatement = conn.prepareStatement(queryArticleSQL.toString());
			int i = 0;
			for (String postId : postIdList) {
				int parameterIndex = i+1;
				preparedStatement.setObject(parameterIndex, postId);
				i++;
			}
			
//			int pageNumIndex = i + 1;
//			preparedStatement.setInt(pageNumIndex, ((pageNum - 1) * pageSize));
//			i++;
//			
//			int pageSizeIndex = i + 1;
//			preparedStatement.setInt(pageSizeIndex, (pageSize));
//			i++;
			
			LOGGER.debug("ps_queryArticleSQL = " + preparedStatement.toString());

			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				ArticleModel.Article article = new ArticleModel().new Article();
				article.setPost_id(rs.getString("id"));
				article.setUrl(rs.getString("url"));
				article.setTitle(rs.getString("title"));
				article.setContent(rs.getString("content")); // Add Content
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

	protected String requestBody(HttpServletRequest request) {
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader((ServletInputStream) request.getInputStream(), "utf-8"));
			StringBuffer sb = new StringBuffer("");
			String temp;
			while ((temp = br.readLine()) != null) {
				sb.append(temp);
			}
			br.close();
			String str = StringUtils.trimToEmpty(sb.toString());
			LOGGER.info(str);
			return str;
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		}
		return null;
	}

}
