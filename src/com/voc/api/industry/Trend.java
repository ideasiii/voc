package com.voc.api.industry;

import java.sql.Connection;
import java.util.Iterator;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;

import org.json.JSONArray;
import org.json.JSONObject;

import com.voc.api.RootAPI;
import com.voc.common.ApiResponse;
import com.voc.common.Common;
import com.voc.common.DBUtil;

public class Trend extends RootAPI {

	@Override
	public JSONObject processRequest(HttpServletRequest request) {
		
		Map<String, String[]> paramMap = request.getParameterMap();
		
		if (!hasRequiredParameters(paramMap)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}
		
		final String strStartDate = request.getParameter("start_date");
		final String strEndDate = request.getParameter("end_date");
		final String strTableName = this.getTableName(paramMap);
		String strInterval;
		
		if (!Common.isValidDate(strStartDate, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid start_date.");
		}
		
		if (!Common.isValidDate(strEndDate, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid end_date.");
		}

		if (!strStartDate.equals(strEndDate)) {
				return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid period values.");
		}
		
		if (!hasInterval(paramMap)) {
			strInterval = "daily";
		} else {
			strInterval = request.getParameter("interval");
		}

		String strSelectClause = genSelectClause(strTableName);
		String strWhereClause = genWhereClause(paramMap);
		
		
		
		
		
		JSONObject jobj = new JSONObject();
		JSONArray resArray = new JSONArray();
		
		
		
		
		return jobj;
	}

	private int queryByBrand(final String strSelectClause,   final JSONArray out) {
		final Connection conn = DBUtil.getConn();
		int status = 0;
		
		return status;
	}
	
private String genSelectClause(String strTableName) {
		
		StringBuilder sql= new StringBuilder();
		sql.append("SELECT ");
		sql.append("date, SUM(reputation)reputation");
		sql.append(" From " + strTableName);
		
		return sql.toString();
	}
	
	private String genWhereClause(Map<String, String[]> paramMap) {
		
		StringBuilder sql= new StringBuilder();
		sql.append(" where ");
		
		Iterator<String> itPM = paramMap.keySet().iterator();
		int nCount = 0;
		
		while(itPM.hasNext()) {
			nCount++;
			String paramName = itPM.next();
			String columnName;
			if (paramName.equals("website")) {
				columnName = "website_name";
			} else
			if (paramName.equals("channel")) {
				columnName = "channel_name";
			} else	
			if (paramName.equals("start_date") || paramName.equals("end_date")) {
				columnName = "date";
			} else {
				columnName = paramName;
			}
			if (nCount < paramMap.size()) {
				sql.append(" and ");
			}
		}
		return sql.toString();
	}
	
	
private boolean hasRequiredParameters(Map<String, String[]> paramMap) {
	return paramMap.containsKey("start_date") && paramMap.containsKey("end_date");
}

private boolean hasInterval(Map<String, String[]> paramMap) {
	return paramMap.containsKey("interval");
}









}