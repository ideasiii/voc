package com.voc.api.industry;

import java.sql.Connection;
import java.sql.PreparedStatement;
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

	if (!Common.isValidStartDate(strStartDate, strEndDate, "yyyy-MM-dd")) {
		return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid period values.");
	}
	
		if (!hasInterval(paramMap)) {
			strInterval = Common.INTERVAL_DAILY;
		} else {
			strInterval = request.getParameter("interval");
			if (!Common.isValidInterval(strInterval)) {
				return  ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid interval.");
			}
		}

		String strSelectClause = genSelectClause(strTableName, strInterval);
		String strWhereClause = genWhereClause(paramMap, strInterval);
		
		System.out.println("**************SQL: " + strSelectClause + strWhereClause); 
		
		JSONObject jobj = new JSONObject();
		JSONArray resArray = new JSONArray();
		int nCount = query(paramMap, strSelectClause, strWhereClause, resArray);
		
		if (0 < nCount) {
			jobj = ApiResponse.successTemplate();
			jobj.put("result", resArray);

		} else {
			switch (nCount) {
			case 0:
				jobj = ApiResponse.dataNotFound();
				break;
			default:
				jobj = ApiResponse.byReturnStatus(nCount);
			}
		}
		return jobj;
	}

	private int query(Map<String, String[]> paramMap, final String strSelectClause, final String strWhereClause, final JSONArray out) {
		Connection conn = null;
		PreparedStatement pst = null;
		StringBuffer querySQL = new StringBuffer();
		int status = 0;
		try {
			querySQL.append(strSelectClause);
			querySQL.append(strWhereClause);
			System.out.println("****************" + querySQL.toString()); 
			
			conn = DBUtil.getConn();
			pst = conn.prepareStatement(querySQL.toString());
			setWhereClauseValues(pst, paramMap);
			
			
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (conn != null) {
				DBUtil.closeConn(conn);
			}
		}
		return status;
	}
	
private String genSelectClause(String strTableName, String strInterval) {
		
		StringBuffer sql= new StringBuffer();
		if (strInterval.equals(Common.INTERVAL_DAILY)) {
		sql.append("SELECT DATE_FORMAT(date, '%Y-%m-%d') as dailyStr, SUM(reputation) AS count FROM ");
		} else if (strInterval.equals(Common.INTERVAL_MONTHLY)) {
			sql.append("SELECT DATE_FORMAT(date, '%Y-%m') as monthlyStr, SUM(reputation) AS count FROM ");
		}
		sql.append(strTableName).append(" ");
		return sql.toString();
	}
	
	private String genWhereClause(Map<String, String[]> paramMap, String strInterval) {
		
		StringBuffer sql= new StringBuffer();
		Iterator<String> itPM = paramMap.keySet().iterator();
		int i = 0;
		
		while(itPM.hasNext()) {
			String paramName = itPM.next();
			if (paramName.equals("interval"))
			{
				continue;
			}
			String columnName = this.getColumnName(paramName);
	
			if (0 == i)
			{
				sql.append("WHERE ");
			} else {
				sql.append("AND ");
			}
			
			sql.append(columnName);
			if (paramName.equals("start_date")) {
				sql.append(" >= ? ");
			} else if (paramName.equals("end_date")) {
				sql.append(" <= ? ");
			} else {
				sql.append(" = ? ");
			}
			i++;
		}
		if (strInterval.equals(Common.INTERVAL_DAILY)) {
			sql.append(" GROUP BY DATE_FORMAT(date, '%Y-%m-%d')");
		} else if (strInterval.equals(Common.INTERVAL_MONTHLY)) {
			sql.append(" GROUP BY DATE_FORMAT(date, '%Y-%m')");
		}
		return sql.toString();
	}
	
	
	private void setWhereClauseValues(PreparedStatement preparedStatement, Map<String, String[]> parameterMap) throws Exception {
		
		
	}
	
	
	
private boolean hasRequiredParameters(Map<String, String[]> paramMap) {
	return paramMap.containsKey("start_date") && paramMap.containsKey("end_date");
}

private boolean hasInterval(Map<String, String[]> paramMap) {
	return paramMap.containsKey("interval");
}









}