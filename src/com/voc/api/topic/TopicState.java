package com.voc.api.topic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voc.api.RootAPI;
import com.voc.common.ApiResponse;
import com.voc.common.Common;
import com.voc.common.DBUtil;

public class TopicState extends RootAPI {
	private static final Logger LOGGER = LoggerFactory.getLogger(TopicState.class);
	private String strId;
	private int nCount = 0;
	
	@Override
	public String processRequest(HttpServletRequest request) {
		
		requestParams(request);
		JSONArray resArray = new JSONArray();
		nCount = query(resArray);
		
		if (0 == nCount) {
			return ApiResponse.error(ApiResponse.STATUS_DATA_NOT_FOUND).toString();
			
		} else if (0 < nCount && null != resArray) {
			JSONObject jobj = ApiResponse.successTemplate();
			jobj.put("result", resArray);
			LOGGER.info("response: " + jobj.toString());
			return jobj.toString();
		}
		return ApiResponse.unknownError().toString();
	}
	
	private JSONObject requestParams(HttpServletRequest request) {
		String strJson = requestBody(request);
		JSONObject jobj = new JSONObject(strJson);
		strId = jobj.getString("id");
		if (StringUtils.isBlank(strId)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}
		return null;
	}
	
	private int query(JSONArray out) {
		String sql = "SELECT * FROM " + TABLE_TOPIC_KEYWORD_JOB_LIST + " WHERE `_id` = " + strId;
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		
		try {
			conn = DBUtil.getConn();
			pst = conn.prepareStatement(sql);
			rs = pst.executeQuery();
			
			String strPstSQL = pst.toString();
			LOGGER.info("strPstSQL: " + strPstSQL);
			
			while (rs.next()) {
				nCount++;
				JSONObject jobj = new JSONObject();
				jobj.put("id", rs.getInt("_id"));
				jobj.put("user", rs.getString("user"));
				jobj.put("project_name", rs.getString("project_name"));
				jobj.put("topic", rs.getString("topic"));
				jobj.put("keyword", rs.getString("keyword"));
				jobj.put("start_date", rs.getString("start_date"));
				jobj.put("end_date", rs.getString("end_date"));
				jobj.put("create_time", rs.getString("create_time"));
				jobj.put("state", rs.getString("state"));
				jobj.put("progress", rs.getInt("progress"));
				out.put(jobj);
			}
			return nCount;
			
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, pst, conn);
		}
		return Common.ERR_EXCEPTION;
	}
	
}
