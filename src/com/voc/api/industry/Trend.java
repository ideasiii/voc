package com.voc.api.industry;

import java.util.Iterator;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;

import org.json.JSONArray;
import org.json.JSONObject;

import com.voc.api.RootAPI;
import com.voc.common.ApiResponse;
import com.voc.common.Common;

public class Trend extends RootAPI {

	@Override
	public JSONObject processRequest(HttpServletRequest request) {
		
		if (!hasRequiredParameters(request)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}
		
		final String strStartDate = request.getParameter("start_date");
		final String strEndDate = request.getParameter("end_date");
		String strInterval;
		String strTableName;
		
		if (!Common.isValidDate(strStartDate, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid start_date.");
		}
		
		if (!Common.isValidDate(strEndDate, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid end_date.");
		}

		if (!strStartDate.equals(strEndDate)) {
				return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid period values.");
		}
		
		if (!hasInterval(request)) {
			strInterval = "daily";
		} else {
			strInterval = request.getParameter("interval");
		}

		JSONObject jobj = new JSONObject();
		JSONArray resArray = new JSONArray();
		
		
		
		
		return jobj;
	}

	private int queryByBrand( , final JSONArray out) {
		
		
		
	}
	
private String genSelectClause(String strTableName) {
		
		StringBuilder query= new StringBuilder();
		query.append("SELECT ");
		query.append("date, SUM(reputation)reputation");
		query.append(" From " + strTableName);
		
		return query.toString();
	}
	
	private String genWhereClause(Map<String, String[]> paramMap) {
		
		StringBuilder query= new StringBuilder();
		query.append(" where ");
		
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
				query.append(" and ");
			}
		}
		return query.toString();
	}
	
	
private boolean hasRequiredParameters(final HttpServletRequest request) {
	Map<String, String[]> paramMap = request.getParameterMap();
	return paramMap.containsKey("start_date") && paramMap.containsKey("end_date");
}

private boolean hasInterval(final HttpServletRequest request) {
	Map<String, String[]> paramMap = request.getParameterMap();
	return paramMap.containsKey("interval");
}

private boolean hasProduct(final HttpServletRequest request) {
	Map<String, String[]> paramMap = request.getParameterMap();
	return paramMap.containsKey("product") || paramMap.containsKey("series");
}









}