package com.voc.api.topic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

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
 * 新增自訂主題任務: 將用戶新增的自訂主題關鍵字新增至任務列表中
 * 
 * http://localhost:8080/voc/topic/add-topic.jsp
 * 
 * Example: the post data in body:
 * {
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
public class AddTopic extends RootAPI {
	private static final Logger LOGGER = LoggerFactory.getLogger(AddTopic.class);
	private int _id; // inserted id
	
	private String user;
	private String project_name;
	private String topic;
	private String keyword;
	private String start_date;
	private String end_date;

	@Override
	public String processRequest(HttpServletRequest request) {
		this.requestAndTrimParams(request);
		JSONObject errorResponse = this.validateParams();
		if (errorResponse != null) {
			LOGGER.info(errorResponse.toString());
			return errorResponse.toString();
		}
		boolean isSuccess = this.insertTopicKeywordJobList(this.user, this.project_name, this.topic, this.keyword, this.start_date, this.end_date);
		if (isSuccess) {
			JSONObject successObj = ApiResponse.successTemplate();
			successObj.put("id", this._id);
			return successObj.toString();
		}
		return ApiResponse.unknownError().toString();
	}
	
	private void requestAndTrimParams(HttpServletRequest request) {
		
			try {
				this.user = StringUtils.trimToEmpty(request.getParameter("user"));
				this.project_name = StringUtils.trimToEmpty(request.getParameter("project_name"));
				this.topic = StringUtils.trimToEmpty(request.getParameter("topic"));
				this.keyword = StringUtils.trimToEmpty(request.getParameter("keyword"));
				this.start_date = StringUtils.trimToEmpty(request.getParameter("start_date"));
				this.end_date = StringUtils.trimToEmpty(request.getParameter("end_date"));
			} catch (Exception e) {
				e.printStackTrace();
		}
	
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
		
		if (this.queryTopicKeywordJobListCnt(this.user, this.project_name, this.topic) > 0) {
			return ApiResponse.error(ApiResponse.STATUS_CONFLICTS);
		}
		
		return null;
	}
	
	private int queryTopicKeywordJobListCnt(String user, String project_name, String topic) {
		int recordCnt = 0;
		Connection conn = null;
		PreparedStatement pStmt = null;
		ResultSet rs = null;
		String sqlStr = "SELECT _id FROM " + TABLE_TOPIC_KEYWORD_JOB_LIST + " WHERE user = ? AND project_name = ? AND topic = ?";
		try {
			conn = DBUtil.getConn();
			pStmt = conn.prepareStatement(sqlStr);
			pStmt.setObject(1, user);
            pStmt.setObject(2, project_name);
            pStmt.setObject(3, topic);
			rs = pStmt.executeQuery();
			while (rs.next()) {
				String _id = rs.getString("_id");
				LOGGER.info("_id = " + _id);
				recordCnt++;
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, pStmt, conn);
		}
		if (recordCnt > 0) {
			LOGGER.info("Duplicated recordCnt = " + recordCnt);
		}
		return recordCnt;
	}

	private boolean insertTopicKeywordJobList(String user, String project_name, String topic, String keyword, String start_date, String end_date) {
		Connection conn = null;
		PreparedStatement pStmt = null;
		String sqlStr="INSERT INTO " + TABLE_TOPIC_KEYWORD_JOB_LIST + " (user, project_name, topic, keyword, start_date, end_date, article_start_date, article_end_date, create_time, state) values (?, ?, ?, ?, ?, ?, ?, ?, ? ,?)";
		String article_start = null;
		
		try {
		Calendar article_start_gc = new GregorianCalendar();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date ed = sdf.parse(end_date);
		article_start_gc.setTime(ed);
		article_start_gc.add(Calendar.DAY_OF_YEAR, -7);
		article_start = sdf.format(article_start_gc.getTime());
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
		
		try {
			conn = DBUtil.getConn();
			conn.setAutoCommit(false);
			
			pStmt = conn.prepareStatement(sqlStr, PreparedStatement.RETURN_GENERATED_KEYS);
			pStmt.setObject(1, user);
            pStmt.setObject(2, project_name);
            pStmt.setObject(3, topic);
            pStmt.setObject(4, keyword);
            pStmt.setObject(5, start_date);
            pStmt.setObject(6, end_date);
            pStmt.setObject(7, start_date); // article_start: end_date -7
            pStmt.setObject(8, end_date);
            pStmt.setObject(9, new Date()); // create_time: 預設寫現在時間
            pStmt.setObject(10, "尚未分析"); // state: 預設寫 "尚未分析"
            pStmt.executeUpdate();
            
			ResultSet generatedKeys = pStmt.getGeneratedKeys();
			if (generatedKeys.next()) {
				this._id = generatedKeys.getInt(1);
			} else {
				try {
					conn.rollback();
				} catch (Exception e) {
					e.printStackTrace();
				}
				LOGGER.error("INSERT failed, no ID obtained.");
				return false;
			}
			conn.commit();
            LOGGER.info("INSERT OK!!!");
			return true;
		} catch (Exception e) {
			try {
				conn.rollback();
			} catch (Exception e2) {
				e2.printStackTrace();
			}
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.closePreparedStatement(pStmt);
			DBUtil.closeConn(conn);
		}
		return false;
	}

}
