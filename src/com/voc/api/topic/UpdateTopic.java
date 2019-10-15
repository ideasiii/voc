package com.voc.api.topic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;

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
 * Request: POST: ==>http://localhost:8080/voc/topic/update-topic.jsp
 * 
 * Data Parameters in body: example: { "id": "12" "user": "ibuzz",
 * "project_name": "母嬰專案", "topic": "尿布", "keyword": "拉拉褲;輕柔;尿溼顯示",
 * "start_date": "2018-12-01", "end_date": "2018-12-31" }
 * 
 * Requirement Change: 
 * 1.新增ouput欄位:state(分析狀態)、progress(分析進度)
 * 2.若input參數含有user,project_name,topic,三者任一則需一併update table:
 * Topic_reputation中對應的欄位
 * 3.移除會把update_time SET成null的動作
 * 
 */
public class UpdateTopic extends RootAPI {
	private static final Logger LOGGER = LoggerFactory.getLogger(UpdateTopic.class);
	private String id;
	private String user;
	private String project_name;
	private String topic;
	private String keyword;
	private String start_date;
	private String end_date;
	private String article_start_date;
	private String article_end_date;
	private String state;
	private String progress;
	
	private String origUser;
	private String origProjectName;
	private String origTopic;

	@Override
	public String processRequest(HttpServletRequest request) {
		JSONObject initErrorResponse = this.requestAndInitValidateParams(request);
		if (initErrorResponse != null) {
			LOGGER.info(initErrorResponse.toString());
			return initErrorResponse.toString();
		}
		int recordCnt = this.findById(this.id);
		if (recordCnt == 0) {
			String errorMsg = ApiResponse
					.error(ApiResponse.STATUS_DATA_NOT_FOUND, "The record you requested does not exist to be updated!")
					.toString();
			LOGGER.info(errorMsg);
			return errorMsg;
		}
		JSONObject errorResponse = this.validateParams();
		if (errorResponse != null) {
			LOGGER.info(errorResponse.toString());
			return errorResponse.toString();
		}

		if (hasParameters(request)) {
			boolean updatedReputation = this.updateTopicReputation();
			boolean updatedJobList = this.updateTopicKeywordJobList();
			if (updatedReputation && updatedJobList) {
				return ApiResponse.successTemplate().toString();
			}
			return ApiResponse.unknownError().toString();
		} else {
			
			boolean isSuccess = this.updateTopicKeywordJobList();
			if (isSuccess) {
				return ApiResponse.successTemplate().toString();
			}
		}
		return ApiResponse.unknownError().toString();
	}

	private JSONObject requestAndInitValidateParams(HttpServletRequest request) {

		this.id = StringUtils.trimToEmpty(request.getParameter("id"));
		if (StringUtils.isBlank(this.id)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER, "Missing parameter of id.");
		}
		this.user = StringUtils.trimToEmpty(request.getParameter("user"));
		this.project_name = StringUtils.trimToEmpty(request.getParameter("project_name"));
		this.topic = StringUtils.trimToEmpty(request.getParameter("topic"));
		this.keyword = StringUtils.trimToEmpty(request.getParameter("keyword"));
		this.start_date = StringUtils.trimToEmpty(request.getParameter("start_date"));
		this.end_date = StringUtils.trimToEmpty(request.getParameter("end_date"));
		this.state = StringUtils.trimToEmpty(request.getParameter("state"));
		this.progress = StringUtils.trimToEmpty(request.getParameter("progress"));

		/**
		 * if (StringUtils.isBlank(this.user) && StringUtils.isBlank(this.project_name)
		 * && StringUtils.isBlank(this.topic) && StringUtils.isBlank(this.keyword) &&
		 * StringUtils.isBlank(this.start_date) && StringUtils.isBlank(this.end_date)) {
		 * return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER); }
		 **/
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
				String article_start_date = rs.getString("article_start_date");
				String article_end_date = rs.getString("article_end_date");
				String state = rs.getString("state");
				String progress = rs.getString("progress");
				
				String origUser = rs.getString("user");
				String origProjectName =rs.getString("project_name");
				String origTopic = rs.getString("topic");

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
				if (StringUtils.isBlank(this.article_start_date)) {
					this.article_start_date = article_start_date;
				}
				if (StringUtils.isBlank(this.article_end_date)) {
					this.article_end_date = article_end_date;
				}
				if (StringUtils.isBlank(this.state)) {
					this.state = state;
				}
				if (StringUtils.isBlank(this.progress)) {
					this.progress = progress;
				}
				if (StringUtils.isBlank(this.origUser)) {
					this.origUser = origUser;
				}
				if (StringUtils.isBlank(this.origProjectName)) {
					this.origProjectName = origProjectName;
				}
				if (StringUtils.isBlank(this.origTopic)) {
					this.origTopic = origTopic;
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
		/**
		 * if (StringUtils.isBlank(this.user) || StringUtils.isBlank(this.project_name)
		 * || StringUtils.isBlank(this.topic) || StringUtils.isBlank(this.keyword) ||
		 * StringUtils.isBlank(this.start_date) || StringUtils.isBlank(this.end_date)) {
		 * return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER); }
		 */
		if (StringUtils.isBlank(this.end_date)) {
			this.article_end_date = this.end_date;
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
		String sqlStr = "UPDATE " + TABLE_TOPIC_KEYWORD_JOB_LIST
				+ " SET user=?, project_name=?, topic=?, keyword=?, start_date=?, end_date=?, article_start_date=?, article_end_date=?, state=?, progress=? WHERE _id=?";

		try {
			Calendar article_start_gc = new GregorianCalendar();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date ed = sdf.parse(end_date);
			article_start_gc.setTime(ed);
			article_start_gc.add(Calendar.DAY_OF_YEAR, -7);
			article_start_date = sdf.format(article_start_gc.getTime());
		} catch (ParseException e1) {
			e1.printStackTrace();
		}

		try {
			conn = DBUtil.getConn();
			pStmt = conn.prepareStatement(sqlStr);
			pStmt.setObject(1, this.user);
			pStmt.setObject(2, this.project_name);
			pStmt.setObject(3, this.topic);
			pStmt.setObject(4, this.keyword);
			pStmt.setObject(5, this.start_date);
			pStmt.setObject(6, this.end_date);
			pStmt.setObject(7, start_date); // article_start_date: end_date -7
			pStmt.setObject(8, this.end_date);
			pStmt.setObject(9, this.state); // state
			pStmt.setObject(10, this.progress); // progress
			pStmt.setObject(11, this.id);
			pStmt.execute();
			LOGGER.info("Table: " + TABLE_TOPIC_KEYWORD_JOB_LIST + " UPDATE OK!!!");
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

	private boolean hasParameters(HttpServletRequest request) {
		Map paramMap = request.getParameterMap();
		return paramMap.containsKey("user") || paramMap.containsKey("project_name") || paramMap.containsKey("topic");
	}

	private boolean updateTopicReputation() {
		Connection conn = null;
		PreparedStatement pStmt = null;
		String sqlStr = "UPDATE " + TABLE_TOPIC_REPUTATION
				+ " SET user=?, project_name=?, topic=? WHERE user=? AND project_name=? AND topic=?";

		try {
			conn = DBUtil.getConn();
			pStmt = conn.prepareStatement(sqlStr);
			pStmt.setObject(1, this.user);
			pStmt.setObject(2, this.project_name);
			pStmt.setObject(3, this.topic);
			pStmt.setObject(4, this.origUser);
			pStmt.setObject(5, this.origProjectName);
			pStmt.setObject(6, this.origTopic);
			pStmt.execute();
			LOGGER.info("Table: " + TABLE_TOPIC_REPUTATION + " UPDATE OK!!!");
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
