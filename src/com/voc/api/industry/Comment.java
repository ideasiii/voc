package com.voc.api.industry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voc.api.RootAPI;
import com.voc.common.ApiResponse;
import com.voc.common.DBUtil;

public class Comment extends RootAPI {
	private static final Logger LOGGER = LoggerFactory.getLogger(Comment.class);
	private Map<String, String[]> paramMap;
	private String strPostId;
	int nPageNum = 1;
	int nPageSize = 100;
	private final String strTableName = TABLE_COMMENT_LIST; 
	
	@Override
	public String processRequest(HttpServletRequest request) {
		paramMap = request.getParameterMap();
		
		if (!hasRequiredParameters(paramMap)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER).toString();
		}
		requestParams(request);
		JSONObject errorResponse = validate();
		if (null != errorResponse) {
			return errorResponse.toString();
		}
		
		JSONArray resArray = query();
		
		if (null != resArray) {
			JSONObject jobj = ApiResponse.successTemplate();
			jobj.put("total", resArray.length());
			jobj.put("page_num", nPageNum);
			jobj.put("page_size", nPageSize);
			jobj.put("result", resArray);
			LOGGER.info("response: "+ jobj.toString());
			return jobj.toString();
		}
		return ApiResponse.unknownError().toString();
	}

	private boolean hasRequiredParameters(Map<String, String[]> paramMap) {
		return paramMap.containsKey("post_id");
	}
	
	private void requestParams(HttpServletRequest request) {
		
		strPostId = request.getParameter("post_id");
		
		String strPageNum = request.getParameter("page_num");
		if (!StringUtils.isBlank(strPageNum)) {
			try {
			this.nPageNum = Integer.parseInt(strPageNum);
			} catch (Exception e) {
				LOGGER.error(e.getMessage());
			}
		}
		
		String strPageSize = request.getParameter("page_size");
		if (!StringUtils.isBlank(strPageSize)) {
			try {
			this.nPageSize = Integer.parseInt(strPageSize);
			} catch (Exception e) {
				LOGGER.error(e.getMessage());
			}
		}
	}
	
	private JSONObject validate() {
		if (StringUtils.isBlank(strPostId)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}
		if (nPageNum < 1) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid page_num.");
		}
		if (nPageSize < 1) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid page_size.");
		}
		return null;
	}
	
	private JSONArray query() {
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
	
		try {
			conn = DBUtil.getConn();
			pst = conn.prepareStatement(genSelecttSQL());
			setWhereClauseValues(pst);
	
			String strPstSQL = pst.toString();
			LOGGER.info("strPstSQL: " + strPstSQL);
			
			rs = pst.executeQuery();
			JSONArray out = new JSONArray();
			while (rs.next()) {
				JSONObject jobj = new JSONObject();
				jobj.put("comment_id", rs.getString("comment_id"));
				jobj.put("content", rs.getString("content"));
				jobj.put("author", rs.getString("author"));
				jobj.put("date", rs.getString("date"));
				LOGGER.info("date: " +  rs.getTimestamp("date"));
				out.put(jobj);
			}
			return out;
			
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, pst, conn);
		}
		return null;
	}
	
	private String genSelecttSQL() {
		StringBuffer sql = new StringBuffer();
		sql.append("SELECT id AS comment_id , content, author, DATE_FORMAT(date, '%Y-%m-%d %H:%i:%s') AS date ");
		sql.append("FROM ").append(strTableName).append(" ");
		sql.append("WHERE post_id = ? ");
		sql.append("ORDER BY date DESC ");
		sql.append("LIMIT ?, ?");
		
		LOGGER.info("SQL : " + sql.toString());
		return sql.toString();
	}
	
	private void setWhereClauseValues(PreparedStatement pst) throws Exception {
		int idx = 1;
		pst.setObject(idx++, strPostId);
		pst.setObject(idx++, (nPageNum - 1) * nPageSize);
		pst.setObject(idx++, nPageSize);
	}
	
	
}
