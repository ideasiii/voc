package com.voc.api.topic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voc.api.RootAPI;
import com.voc.common.ApiResponse;
import com.voc.common.Common;
import com.voc.common.DBUtil;

/**
 * 更新自訂主題任務: 將用戶的自訂主題關鍵字資料自任務列表做更新
 * 
 * Request: POST: 
 * ==>http://localhost:8080/voc/topic/update-topic.jsp
 * 
 * Data Parameters in body: example:
 * {
 *   "id": "12"
 *   "user": "ibuzz",
 *   "project_name": "母嬰專案",
 *   "topic": "尿布",
 *   "keyword": "拉拉褲;輕柔;尿溼顯示",
 *   "start_date": "2018-12-01",
 *   "end_date": "2018-12-31"
 * }
 * 
 * 
 */
public class UpdateTopic extends RootAPI {
	private static final Logger LOGGER = LoggerFactory.getLogger(UpdateTopic.class);
	private JSONObject requestJsonObj = new JSONObject();
	private String id;
	private String user;
	private String project_name;
	private String topic;
	private String keyword;
	private String start_date;
	private String end_date;

	@Override
	public String processRequest(HttpServletRequest request) {
		JSONObject initErrorResponse = this.requestAndInitValidateParams(request);
		if (initErrorResponse != null) {
			LOGGER.info(initErrorResponse.toString());
			return initErrorResponse.toString();
		}
		int recordCnt = this.findById(this.id);
		if (recordCnt == 0) {
			String errorMsg = ApiResponse.error(ApiResponse.STATUS_DATA_NOT_FOUND, "The record you requested does not exist to be updated!").toString();
			LOGGER.info(errorMsg);
			return errorMsg;
		}
		JSONObject errorResponse = this.validateParams();
		if (errorResponse != null) {
			LOGGER.info(errorResponse.toString());
			return errorResponse.toString();
		}
		boolean isSuccess = this.updateTopicKeywordJobList();
		if (isSuccess) {
			return ApiResponse.successTemplate().toString();
		}
		return ApiResponse.unknownError().toString();
	}

	private JSONObject requestAndInitValidateParams(HttpServletRequest request) {
		String requestJsonStr = this.requestBody(request);
		if (StringUtils.isBlank(requestJsonStr)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}
		if (StringUtils.isNotBlank(requestJsonStr)) {
			try {
				this.requestJsonObj = new JSONObject(requestJsonStr);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		if (requestJsonObj.has("id")) {
			this.id = StringUtils.trimToEmpty(this.requestJsonObj.getString("id"));
		}
		if (StringUtils.isBlank(this.id)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER, "Missing parameter of id.");
		}
		
		if (requestJsonObj.has("user")) {
			this.user = StringUtils.trimToEmpty(requestJsonObj.getString("user"));
		}
		if (requestJsonObj.has("project_name")) {
			this.project_name = StringUtils.trimToEmpty(requestJsonObj.getString("project_name"));
		}
		if (requestJsonObj.has("topic")) {
			this.topic = StringUtils.trimToEmpty(requestJsonObj.getString("topic"));
		}
		if (requestJsonObj.has("keyword")) {
			this.keyword = StringUtils.trimToEmpty(requestJsonObj.getString("keyword"));
		}
		if (requestJsonObj.has("start_date")) {
			this.start_date = StringUtils.trimToEmpty(requestJsonObj.getString("start_date"));
		}
		if (requestJsonObj.has("end_date")) {
			this.end_date = StringUtils.trimToEmpty(requestJsonObj.getString("end_date"));
		}
		
		if (StringUtils.isBlank(this.user) && StringUtils.isBlank(this.project_name) && StringUtils.isBlank(this.topic)
				&& StringUtils.isBlank(this.keyword) && StringUtils.isBlank(this.start_date) && StringUtils.isBlank(this.end_date)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}
		
		return null;
	}
	
	private int findById(String id) {
		int recordCnt = 0;
		Connection conn = null;
		PreparedStatement pStmt = null;
		ResultSet rs = null;
		String sqlStr = "SELECT * FROM " + TABLE_TOPIC_KEYWORD_JOB_LIST + " WHERE _id = ?";
		try {
			conn = DBUtil.getConn();
			pStmt = conn.prepareStatement(sqlStr);
			pStmt.setObject(1, this.id);
			rs = pStmt.executeQuery();
			if (rs.next()) {
				recordCnt++;
				String user = rs.getString("user");
				String project_name = rs.getString("project_name");
				String topic = rs.getString("topic");
				String keyword = rs.getString("keyword");
				String start_date = rs.getString("start_date");
				String end_date = rs.getString("end_date");
				
				if (StringUtils.isBlank(this.user)) {
					this.user = user;
				}
				if (StringUtils.isBlank(this.project_name)) {
					this.project_name = project_name;
				}
				if (StringUtils.isBlank(this.topic)) {
					this.topic = topic;
				}
				if (StringUtils.isBlank(this.keyword)) {
					this.keyword = keyword;
				}
				if (StringUtils.isBlank(this.start_date)) {
					this.start_date = start_date;
				}
				if (StringUtils.isBlank(this.end_date)) {
					this.end_date = end_date;
				}
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, pStmt, conn);
		}
		return recordCnt;
	}

	private JSONObject validateParams() {
		if (StringUtils.isBlank(this.user) || StringUtils.isBlank(this.project_name) || StringUtils.isBlank(this.topic)
				|| StringUtils.isBlank(this.keyword) || StringUtils.isBlank(this.start_date) || StringUtils.isBlank(this.end_date)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}
		
		if (!Common.isValidDate(this.start_date, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid start_date.");
		}
		this.start_date = Common.formatDate(this.start_date, "yyyy-MM-dd");
		
		if (!Common.isValidDate(this.end_date, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid end_date.");
		}
		this.end_date = Common.formatDate(this.end_date, "yyyy-MM-dd");
		
		if (!Common.isValidStartDate(this.start_date, this.end_date, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid period values.");
		}
		
		return null;
	}
	
	private boolean updateTopicKeywordJobList() {
		Connection conn = null;
		PreparedStatement pStmt = null;
		String sqlStr="UPDATE " + TABLE_TOPIC_KEYWORD_JOB_LIST + " SET user=?, project_name=?, topic=?, keyword=?, start_date=?, end_date=? WHERE _id=?";
        try {
			conn = DBUtil.getConn();
			pStmt = conn.prepareStatement(sqlStr);
			pStmt.setObject(1, this.user);
            pStmt.setObject(2, this.project_name);
            pStmt.setObject(3, this.topic);
            pStmt.setObject(4, this.keyword);
            pStmt.setObject(5, this.start_date);
            pStmt.setObject(6, this.end_date);
            pStmt.setObject(7, this.id);
            pStmt.execute();
            LOGGER.info("UPDATE OK!!!");
			return true;
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.closePreparedStatement(pStmt);
			DBUtil.closeConn(conn);
		}
		return false;
	}

}
