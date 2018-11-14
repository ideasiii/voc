package com.voc.api.industry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.voc.api.RootAPI;
import com.voc.api.model.ArticleModel;
import com.voc.common.ApiResponse;
import com.voc.common.DBUtil;

/**
 * 以文章 ID 查詢文章相關資訊。
 * 
 * Method	URL
 * GET	/industry/article.jsp
 * 
 * Type	Name	Values	Description
 * query		*post_id	string	文章 ID
 * query		*api_key	string	MORE API token
 * 
 * EX: SQL:
 * 	SELECT id, url, title, content, author, DATE_FORMAT(date, '%Y-%m-%d %H:%i:%s') AS date, channel_name, comment_count 
 * 	FROM ibuzz_voc.post_list 
 * 	WHERE id = '5b2060dfa85d0a030495e17f';
 * 
 * EX: API URL:
 * http://localhost:8080/voc/industry/article.jsp?post_id=5b2060dfa85d0a030495e17f
 *
 */
public class Article extends RootAPI {
	private static final Logger LOGGER = LoggerFactory.getLogger(Article.class);
	private static final Gson GSON = new Gson();
	private String postId; // post_id

	@Override
	public String processRequest(HttpServletRequest request) {
		this.postId = StringUtils.trimToEmpty(request.getParameter("post_id"));
		if (StringUtils.isBlank(this.postId)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER).toString();
		}
		
		ArticleModel.Article article = this.queryArticleById(this.postId);
		if(article != null) {
			ArticleModel articleModel = new ArticleModel();
			articleModel.setSuccess(true);
			articleModel.setResult(article);
			String responseJsonStr = GSON.toJson(articleModel);
			LOGGER.info("responseJsonStr=" + responseJsonStr);
			return responseJsonStr;
		}
		return ApiResponse.unknownError().toString();
	}
	
	private ArticleModel.Article queryArticleById(String id) {
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		try {
			StringBuffer queryArticleSQL = new StringBuffer();
			queryArticleSQL.append("SELECT id, url, title, content, author, DATE_FORMAT(date, '%Y-%m-%d %H:%i:%s') AS date, channel_name, comment_count ");
			queryArticleSQL.append("FROM ").append(TABLE_POST_LIST).append(" ");
			queryArticleSQL.append("WHERE id = ? ");
			
			conn = DBUtil.getConn();
			preparedStatement = conn.prepareStatement(queryArticleSQL.toString());
			preparedStatement.setObject(1, this.postId);
			LOGGER.debug("psSQL = " + preparedStatement.toString());
			
			ArticleModel.Article article = new ArticleModel().new Article();
			rs = preparedStatement.executeQuery();
			if (rs.next()) {
				article.setPost_id(rs.getString("id"));
				article.setUrl(rs.getString("url"));
				article.setTitle(rs.getString("title"));
				article.setContent(rs.getString("content"));
				article.setAuthor(rs.getString("author"));
				article.setDate(rs.getString("date"));
				article.setChannel(rs.getString("channel_name"));
				article.setComment_count(rs.getInt("comment_count"));
			}
			return article;
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
		} finally {
			DBUtil.close(rs, preparedStatement, conn);
		}
		return null;
	}

}
