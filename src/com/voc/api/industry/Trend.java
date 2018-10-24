package com.voc.api.industry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.apache.catalina.util.ParameterMap;
import org.json.JSONArray;
import org.json.JSONObject;

import com.voc.api.RootAPI;
import com.voc.common.ApiResponse;
import com.voc.common.ApiUtil;
import com.voc.common.Common;
import com.voc.common.DBUtil;
import com.voc.enums.industry.EnumTrend;

public class Trend extends RootAPI {

	@Override
	public JSONObject processRequest(HttpServletRequest request) {

		Map<String, String[]> paramMap = request.getParameterMap();

		if (!hasRequiredParameters(paramMap)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}

		final String strStartDate = request.getParameter("start_date");
		final String strEndDate = request.getParameter("end_date");
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
				return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid interval.");
			}
		}

		paramMap = this.adjustParameterOrder(paramMap);
		String strSelectClause = genSelectClause(paramMap, strInterval);
		String strWhereClause = genWhereClause(paramMap, strInterval);

		System.out.println("**************SQL: " + strSelectClause + strWhereClause);

		JSONObject jobj = new JSONObject();
		JSONArray resArray = new JSONArray();
		boolean querySuccess = query(paramMap, strSelectClause, strWhereClause, strInterval, resArray);

		if (querySuccess) {
			
			if (strInterval.equals(Common.INTERVAL_DAILY)) {
				List<String> dailyList = ApiUtil.getDailyList(strStartDate, strEndDate);
				System.out.println("******DailyList: " + dailyList);
				
				
				
			
					
					
				
			}
			
			
			
			
			jobj = ApiResponse.successTemplate();
			jobj.put("result", resArray);

		} else {
				jobj = ApiResponse.unknownError();
		}
		return jobj;
	}

	private boolean query(Map<String, String[]> paramMap, final String strSelectClause, final String strWhereClause, final String strInterval,
			final JSONArray out) {
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		String date = null;
		StringBuffer querySQL = new StringBuffer();
		try {
			querySQL.append(strSelectClause);
			querySQL.append(strWhereClause);
			System.out.println("****************" + querySQL.toString());

			conn = DBUtil.getConn();
			pst = conn.prepareStatement(querySQL.toString());
			setWhereClauseValues(pst, paramMap);

			Map<String, JSONArray> itemMap = new HashMap<>();
			rs = pst.executeQuery();
			
			while (rs.next()) {
				StringBuffer item = new StringBuffer();
				int i = 0;
				for (Map.Entry<String, String[]> entry : paramMap.entrySet()) {
					String paramName = entry.getKey();			
					String columnName = getColumnName(paramName);
					if ("website_id".equals(columnName)) {
						columnName = "website_name";
					} else if ("channel_id".equals(columnName)) {
						columnName = "channel_name";
					}
					if (!"date".equals(columnName)) {
						String str = rs.getString(columnName);;
						if (i == 0) {
							item.append(str);
						} else {
							item.append("-").append(str);
						}
					}
					i++;
				}
				if (strInterval.equals(Common.INTERVAL_MONTHLY)) {
					date = rs.getString("monthlyStr");
				} else {
					date = rs.getString("dailyStr");
				}
				int count = rs.getInt("count"); 
				System.out.println("************** date: "+ date+ " count: "+ count );
				
				JSONObject dataObj = new JSONObject();
				dataObj.put("date", date);
				dataObj.put("count", count);
				
				if (itemMap.get(item.toString()) == null) {
					itemMap.put(item.toString(), new JSONArray());
				}
				JSONArray dataArray = itemMap.get(item.toString());
				dataArray.put(dataObj);
				itemMap.put(item.toString(), dataArray);
			}
			
			for (Map.Entry<String, JSONArray> entry : itemMap.entrySet()) {
			String strItem = entry.getKey();
			JSONArray dataArray = entry.getValue();	
			
			JSONObject resultObj = new JSONObject();
			resultObj.put("item", strItem);
			resultObj.put("data", dataArray);
			out.put(resultObj);
			}
			return true;
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				DBUtil.closeResultSet(rs);
			}
			if (pst != null) {
				DBUtil.closePreparedStatement(pst);
			}
			if (conn != null) {
				DBUtil.closeConn(conn);
			}
		}
		return false;
	}

	private String genSelectClause(Map<String, String[]> paramMap, String strInterval) {

		if (hasInterval(paramMap)) {
			paramMap.remove("interval");
		}
		StringBuffer sql = new StringBuffer();
		sql.append("SELECT ");
		int i = 0;
		for (Map.Entry<String, String[]> entry : paramMap.entrySet()) {
			String paramName = entry.getKey();
			if (isItemParamName(paramName)) {
				String columnName = this.getColumnName(paramName);
				if ("website_id".equals(columnName)) {
					columnName = "website_name";
				} else if ("channel_id".equals(columnName)) {
					columnName = "channel_name";
				}
				if (0 == i) {
					sql.append(columnName);
				} else {
					sql.append(" ,").append(columnName);
				}
				i++;
			}
		}
		String strTableName = getTableName(paramMap);
		if (strInterval.equals(Common.INTERVAL_DAILY)) {
			sql.append(" ,").append("DATE_FORMAT(date, '%Y-%m-%d') AS dailyStr");
		} else if (strInterval.equals(Common.INTERVAL_MONTHLY)) {
			sql.append(" ,").append("DATE_FORMAT(date, '%Y-%m') AS monthlyStr");
		}
		sql.append(" ,").append("SUM(reputation) AS count FROM ").append(strTableName).append(" ");

		return sql.toString();
	}

	private String genWhereClause(Map<String, String[]> paramMap, String strInterval) {
		if (hasInterval(paramMap)) {
			paramMap.remove("interval");
		}
		StringBuffer sql = new StringBuffer();
		int i = 0;
		for (Map.Entry<String, String[]> entry : paramMap.entrySet()) {
			String paramName = entry.getKey();
			String columnName = this.getColumnName(paramName);

			if (0 == i) {
				sql.append("WHERE ");
			} else {
				sql.append("AND ");
			}

			if (paramName.equals("start_date")) {
				sql.append(" DATE_FORMAT(date, '%Y-%m-%d') >= ? ");
			} else if (paramName.equals("end_date")) {
				sql.append(" DATE_FORMAT(date, '%Y-%m-%d') <= ? ");
			} else {
				sql.append(columnName);
				sql.append(" IN (");
				String[] arrValue = entry.getValue();
				for (int c = 0; c < arrValue.length; c++) {
					if (0 == c) {
						sql.append(" ?");
					} else {
						sql.append(" ,?");
					}
				}
				sql.append(") ");
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

	private void setWhereClauseValues(PreparedStatement pst, Map<String, String[]> paramMap) throws Exception {
		if (hasInterval(paramMap)) {
			paramMap.remove("interval");
		}
		int i = 0;
		for (Map.Entry<String, String[]> entry : paramMap.entrySet()) {
			String paramName = entry.getKey();
			String[] arrValue = entry.getValue();
			String value = "";
			if (arrValue != null && arrValue.length > 0) {
				value = arrValue[0];
				if (paramName.equals("start_date") || paramName.equals("end_date")) {
					int parameterIndex = i + 1;
					pst.setObject(parameterIndex, value);
					System.out.println("*********" + parameterIndex + ":" + value); 
					i++;
				} else {
					String[] valueArr = entry.getValue();
					for (String v : valueArr) {
						int parameterIndex = i + 1;
						pst.setObject(parameterIndex, v);
						System.out.println("*********" + parameterIndex + ":" + v);
						i++;
					}
				}
			}
		}

	}

	private boolean hasRequiredParameters(Map<String, String[]> paramMap) {
		return paramMap.containsKey("start_date") && paramMap.containsKey("end_date");
	}

	private boolean hasInterval(Map<String, String[]> paramMap) {
		return paramMap.containsKey("interval");
	}
	
	private Map<String, String[]> adjustParameterOrder(Map<String, String[]> parameterMap) {
		Map<String, String[]> orderedParameterMap = new ParameterMap<>();
		
		String[] paramValues_industry = null;
		String[] paramValues_brand = null;
		String[] paramValues_series = null;
		String[] paramValues_product = null;
		String[] paramValues_source = null;
		String[] paramValues_website = null;
		String[] paramValues_channel = null;
		String[] paramValues_features = null;
		String[] paramValues_startDate = null;
		String[] paramValues_endDate = null;
		
		for (Map.Entry<String, String[]> entry : parameterMap.entrySet()) {
			String paramName = entry.getKey();
			String[] values = entry.getValue();
			EnumTrend enumTrend = EnumTrend.getEnum(paramName);
			if (enumTrend == null) continue; 
			switch (enumTrend) {
			case PARAM_COLUMN_INDUSTRY:
				paramValues_industry = values;
				break;
			case PARAM_COLUMN_BRAND:
				paramValues_brand = values;
				break;
			case PARAM_COLUMN_SERIES:
				paramValues_series = values;
				break;
			case PARAM_COLUMN_PRODUCT:
				paramValues_product = values;
				break;
			case PARAM_COLUMN_SOURCE:
				paramValues_source = values;
				break;
			case PARAM_COLUMN_WEBSITE:
				paramValues_website = values;
				break;
			case PARAM_COLUMN_CHANNEL:
				paramValues_channel = values;
				break;
			case PARAM_COLUMN_FEATURES:
				paramValues_features = values;
				break;
			case PARAM_COLUMN_START_DATE:
				paramValues_startDate = values;
				break;
			case PARAM_COLUMN_END_DATE:
				paramValues_endDate = values;
				break;
			default:
				// Do nothing
				break;
			}
		}
		if (paramValues_industry != null) {
			String paramName = EnumTrend.PARAM_COLUMN_INDUSTRY.getParamName();
			orderedParameterMap.put(paramName, paramValues_industry);
		}
		if (paramValues_brand != null) {
			String paramName = EnumTrend.PARAM_COLUMN_BRAND.getParamName();
			orderedParameterMap.put(paramName, paramValues_brand);
		}
		if (paramValues_series != null) {
			String paramName = EnumTrend.PARAM_COLUMN_SERIES.getParamName();
			orderedParameterMap.put(paramName, paramValues_series);
		}
		if (paramValues_product != null) {
			String paramName = EnumTrend.PARAM_COLUMN_PRODUCT.getParamName();
			orderedParameterMap.put(paramName, paramValues_product);
		}
		if (paramValues_source != null) {
			String paramName = EnumTrend.PARAM_COLUMN_SOURCE.getParamName();
			orderedParameterMap.put(paramName, paramValues_source);
		}
		if (paramValues_website != null) {
			String paramName = EnumTrend.PARAM_COLUMN_WEBSITE.getParamName();
			orderedParameterMap.put(paramName, paramValues_website);
		}
		if (paramValues_channel != null) {
			String paramName = EnumTrend.PARAM_COLUMN_CHANNEL.getParamName();
			orderedParameterMap.put(paramName, paramValues_channel);
		}
		if (paramValues_features != null) {
			String paramName = EnumTrend.PARAM_COLUMN_FEATURES.getParamName();
			orderedParameterMap.put(paramName, paramValues_features);
		}
		if (paramValues_startDate != null) {
			String paramName = EnumTrend.PARAM_COLUMN_START_DATE.getParamName();
			orderedParameterMap.put(paramName, paramValues_startDate);
		}
		if (paramValues_endDate != null) {
			String paramName = EnumTrend.PARAM_COLUMN_END_DATE.getParamName();
			orderedParameterMap.put(paramName, paramValues_endDate);
		}
		return orderedParameterMap;
	}

}