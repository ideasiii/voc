package com.voc.api.topic;

import java.sql.Connection;
import java.sql.PreparedStatement;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voc.api.RootAPI;
import com.voc.common.ApiResponse;
import com.voc.common.DBUtil;

/**
 * Request: POST: 
 * ==>http://localhost:8080/voc/topic/delete-topic.jsp
 * 
 * Data Parameters in body: example:
 * {
 *   "id": "12"
 * }
 *
 */
public class DeleteTopic extends RootAPI {
	private static final Logger LOGGER = LoggerFactory.getLogger(DeleteTopic.class);
	private String id;

	@Override
	public String processRequest(HttpServletRequest request) {
		try {
			this.requestAndTrimParams(request);
			JSONObject errorResponse = this.validateParams();
			if (errorResponse != null) {
				LOGGER.info(errorResponse.toString());
				return errorResponse.toString();
			}
			
			int deletedRowCnt = this.deleteById(this.id);
			if (deletedRowCnt > 0) {
				LOGGER.info("DELETE OK");
				return ApiResponse.successTemplate().toString();
			} else {
				String errorMsg = ApiResponse.error(ApiResponse.STATUS_DATA_NOT_FOUND, "The record you requested does not exist to be deleted!").toString();
				LOGGER.info(errorMsg);
				return errorMsg;
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
		}
		return ApiResponse.unknownError().toString();
	}
	
	private void requestAndTrimParams(HttpServletRequest request) {
		String requestJsonStr = this.requestBody(request);
		JSONObject requestJsonObj = new JSONObject();
		if (StringUtils.isNotBlank(requestJsonStr)) {
			try {
				requestJsonObj = new JSONObject(requestJsonStr);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if (requestJsonObj.has("id")) {
			this.id = StringUtils.trimToEmpty(requestJsonObj.getString("id"));
		}
	}
	
	private JSONObject validateParams() {
		if (StringUtils.isBlank(this.id)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}
		return null;
	}
	
	private int deleteById(String id) {
		int deletedRowCnt = 0;
		Connection conn = null;
		PreparedStatement pStmt = null;
		String sqlStr="DELETE FROM " + TABLE_TOPIC_KEYWORD_JOB_LIST +" WHERE _id=? ";
		try {
			conn = DBUtil.getConn();
			pStmt = conn.prepareStatement(sqlStr);
			pStmt.setObject(1, this.id);
            deletedRowCnt = pStmt.executeUpdate();
        } catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.closePreparedStatement(pStmt);
			DBUtil.closeConn(conn);
		}
		return deletedRowCnt;
	}

}
