package com.voc.api.topic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Date;

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
			return errorResponse.toString();
		}
		boolean isSuccess = this.insertTopicKeywordJobList(this.user, this.project_name, this.topic, this.keyword, this.start_date, this.end_date);
		if (isSuccess) {
			return ApiResponse.successTemplate().toString();
		}
		return ApiResponse.unknownError().toString();
	}
	
	private void requestAndTrimParams(HttpServletRequest request) {
		String jsonStr = this.requestBody(request);
		JSONObject jSONObject = new JSONObject(jsonStr);
		this.user = StringUtils.trimToEmpty(jSONObject.getString("user"));
		this.project_name = StringUtils.trimToEmpty(jSONObject.getString("project_name"));
		this.topic = StringUtils.trimToEmpty(jSONObject.getString("topic"));
		this.keyword = StringUtils.trimToEmpty(jSONObject.getString("keyword"));
		this.start_date = StringUtils.trimToEmpty(jSONObject.getString("start_date"));
		this.end_date = StringUtils.trimToEmpty(jSONObject.getString("end_date"));
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
	
	private boolean insertTopicKeywordJobList(String user, String project_name, String topic, String keyword, String start_date, String end_date) {
		Connection conn = null;
		PreparedStatement pStmt = null;
		String sqlStr="INSERT INTO topic_keyword_job_list (user, project_name, topic, keyword, start_date, end_date, create_time, state) values (?, ?, ?, ?, ?, ?, ?, ?)";
        try {
			conn = DBUtil.getConn();
			pStmt = conn.prepareStatement(sqlStr);
			pStmt.setObject(1, user);
            pStmt.setObject(2, project_name);
            pStmt.setObject(3, topic);
            pStmt.setObject(4, keyword);
            pStmt.setObject(5, start_date);
            pStmt.setObject(6, end_date);
            pStmt.setObject(7, new Date()); // create_time: 預設寫現在時間
            pStmt.setObject(8, "尚未分析"); // state: 預設寫 "尚未分析"
            pStmt.execute();
            LOGGER.info("INSERT OK!!!");
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
