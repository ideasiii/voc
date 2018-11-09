package com.voc.api.industry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.catalina.util.ParameterMap;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voc.api.RootAPI;
import com.voc.common.ApiResponse;
import com.voc.common.ApiUtil;
import com.voc.common.Common;
import com.voc.common.DBUtil;
import com.voc.enums.industry.EnumTrend;

public class Trend extends RootAPI {
	private static final Logger LOGGER = LoggerFactory.getLogger(Trend.class);
	private String selectUpdateTimeSQL;
	private List<String> itemNameList = new ArrayList<>();
	private String strTableName;
	
	@Override
	public String processRequest(HttpServletRequest request) {

		Map<String, String[]> paramMap = request.getParameterMap();
		strTableName = getTableName(paramMap);

		if (!hasRequiredParameters(paramMap)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER).toString();
		}

		final String strStartDate = request.getParameter("start_date");
		final String strEndDate = request.getParameter("end_date");
		String strInterval;

		if (!Common.isValidDate(strStartDate, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid start_date.").toString();
		}

		if (!Common.isValidDate(strEndDate, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid end_date.").toString();
		}

		if (!Common.isValidStartDate(strStartDate, strEndDate, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid period values.").toString();
		}

		if (!hasInterval(paramMap)) {
			strInterval = Common.INTERVAL_DAILY;
		} else {
			strInterval = request.getParameter("interval");
			if (!Common.isValidInterval(strInterval)) {
				return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid interval.").toString();
			}
		}

		paramMap = this.adjustParameterOrder(paramMap);
		JSONObject jobj = new JSONObject();
		JSONArray resArray = new JSONArray();
		boolean querySuccess = query(paramMap, strInterval, strStartDate, strEndDate, resArray);
		LOGGER.info("resArray: "+ resArray );
		String update_time = this.queryUpdateTime(this.selectUpdateTimeSQL);
		
		if (querySuccess) {
			jobj = ApiResponse.successTemplate();
			jobj.put("update_time", update_time);
			jobj.put("result", resArray);
			LOGGER.info("response: "+ jobj.toString());
			
		} else {
				jobj = ApiResponse.unknownError();
		}
		return jobj.toString();
	}

	private boolean query(Map<String, String[]> paramMap, final String strInterval, final String strStartDate, final String strEndDate,
			final JSONArray out) {
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		String date = null;
				
		StringBuffer querySQL = new StringBuffer();
		try {
			querySQL.append(genSelectClause(paramMap, strInterval));
			querySQL.append(genWhereClause(paramMap, strInterval));
			querySQL.append(genGroupByClause(paramMap, strInterval));
			
			LOGGER.info("querySQL: " + querySQL.toString());

			conn = DBUtil.getConn();
			pst = conn.prepareStatement(querySQL.toString());
			setWhereClauseValues(pst, paramMap);

			//get update_time SQL
			String strPstSQL = pst.toString();
			LOGGER.info("strPstSQL: " + strPstSQL);
			this.selectUpdateTimeSQL = "SELECT MAX(DATE_FORMAT(update_time, '%Y-%m-%d %H:%i:%s')) AS " + UPDATE_TIME + strPstSQL.substring(strPstSQL.indexOf(" FROM "), strPstSQL.indexOf(" GROUP BY "));
			LOGGER.info("selectUpdateTimeSQL: " + this.selectUpdateTimeSQL); 
			
			Map<String, Map<String, Integer>> hash_itemName_dataMap = new HashMap<>();
			Map<String, Integer> dataMap = new HashMap<String, Integer>();
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
						String str = rs.getString(columnName);
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
				LOGGER.info("item: " + item.toString() + ", date: " + date + ", count: " + count);
				
				if (hash_itemName_dataMap.get(item.toString()) == null) {
					hash_itemName_dataMap.put(item.toString(), new HashMap<String, Integer>());
				}
				dataMap = hash_itemName_dataMap.get(item.toString());
				dataMap.put(date, count);
			}	
				
				for (String itemName: itemNameList) {
					dataMap = hash_itemName_dataMap.get(itemName);
					JSONArray dataArray = new JSONArray();
					List<String> dateList = null;
					if (strInterval.equals(Common.INTERVAL_MONTHLY)) {
						dateList = ApiUtil.getMonthlyList(strStartDate, strEndDate);
					} else {
						dateList = ApiUtil.getDailyList(strStartDate, strEndDate);
					}
					for (String dataStr : dateList) {
						Integer count = null;
						if (null != dataMap) { 		
							count = dataMap.get(dataStr);
						}
						if (count == null) count = 0;
						JSONObject dataObject = new JSONObject();
						dataObject.put("date", dataStr);
						dataObject.put("count", count);
						dataArray.put(dataObject);
					}
					JSONObject resultObj = new JSONObject();
					resultObj.put("item", itemName);
					resultObj.put("data", dataArray);
					out.put(resultObj);
				}
			return true;
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, pst, conn);
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
		strTableName = getTableName(paramMap);
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
				String[] arrValue = entry.getValue()[0].split(PARAM_VALUES_SEPARATOR);
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
		return sql.toString();
	}
	
	private String genGroupByClause(Map<String, String[]> paramMap, final String strInterval) {
		StringBuffer sql = new StringBuffer();
		StringBuffer groupByColumns = new StringBuffer();
		int i = 0;
		for (Map.Entry<String, String[]> entry : paramMap.entrySet()) {
			String paramName = entry.getKey();
			String columnName = this.getColumnName(paramName);
			if (!"date".equals(columnName)) {
				if (0 == i) {
					groupByColumns.append(columnName);
				} else {
					groupByColumns.append(", ").append(columnName);
				}
			}
		i++;
		}
		
		if (strInterval.equals(Common.INTERVAL_MONTHLY)) {
			sql.append(" GROUP BY DATE_FORMAT(date, '%Y-%m'), ").append(groupByColumns.toString());
		} else if (strInterval.equals(Common.INTERVAL_DAILY)) {
			sql.append(" GROUP BY DATE_FORMAT(date, '%Y-%m-%d'), ").append(groupByColumns.toString());
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
					LOGGER.info(parameterIndex + ":" + value); 
					i++;
				} else {
					String[] valueArr = entry.getValue()[0].split(PARAM_VALUES_SEPARATOR);
					for (String v : valueArr) {
						int parameterIndex = i + 1;
						pst.setObject(parameterIndex, v);
						LOGGER.info("***" + parameterIndex + ":" + v);
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
			if (API_KEY.equals(paramName)) continue;
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
				paramValues_startDate[0] = Common.formatDate(paramValues_startDate[0], "yyyy-MM-dd");
				break;
			case PARAM_COLUMN_END_DATE:
				paramValues_endDate = values;
				paramValues_endDate[0] = Common.formatDate(paramValues_endDate[0], "yyyy-MM-dd");
				break;
			default:
				// Do nothing
				break;
			}
		}
		
		String[] mainItemArr = null;
		String[] secItemArr = null;
		int itemCnt = 0;
	
		if (paramValues_industry != null) {
			String paramName = EnumTrend.PARAM_COLUMN_INDUSTRY.getParamName();
			orderedParameterMap.put(paramName, paramValues_industry);
			if (0 == itemCnt) {
				mainItemArr = paramValues_industry[0].split(PARAM_VALUES_SEPARATOR);
			} else if (1 == itemCnt) {
				secItemArr = paramValues_industry[0].split(PARAM_VALUES_SEPARATOR);
			}
			itemCnt++;
		}
		if (paramValues_brand != null) {
			String paramName = EnumTrend.PARAM_COLUMN_BRAND.getParamName();
			orderedParameterMap.put(paramName, paramValues_brand);
			if (0 == itemCnt) {
				mainItemArr = paramValues_brand[0].split(PARAM_VALUES_SEPARATOR);
			} else if (1 == itemCnt) {
				secItemArr = paramValues_brand[0].split(PARAM_VALUES_SEPARATOR);
			}
			itemCnt++;
		}
		if (paramValues_series != null) {
			String paramName = EnumTrend.PARAM_COLUMN_SERIES.getParamName();
			orderedParameterMap.put(paramName, paramValues_series);
			if (0 == itemCnt) {
				mainItemArr = paramValues_series[0].split(PARAM_VALUES_SEPARATOR);
			} else if (1 == itemCnt) {
				secItemArr = paramValues_series[0].split(PARAM_VALUES_SEPARATOR);
			}
			itemCnt++;
		}
		if (paramValues_product != null) {
			String paramName = EnumTrend.PARAM_COLUMN_PRODUCT.getParamName();
			orderedParameterMap.put(paramName, paramValues_product);
			if (0 == itemCnt) {
				mainItemArr = paramValues_product[0].split(PARAM_VALUES_SEPARATOR);
			} else if (1 == itemCnt) {
				secItemArr = paramValues_product[0].split(PARAM_VALUES_SEPARATOR);
			}
			itemCnt++;
		}
		if (paramValues_source != null) {
			String paramName = EnumTrend.PARAM_COLUMN_SOURCE.getParamName();
			orderedParameterMap.put(paramName, paramValues_source);
			if (0 == itemCnt) {
				mainItemArr = paramValues_source[0].split(PARAM_VALUES_SEPARATOR);
			} else if (1 == itemCnt) {
				secItemArr = paramValues_source[0].split(PARAM_VALUES_SEPARATOR);
			}
			itemCnt++;
		}
		if (paramValues_website != null) {
			String paramName = EnumTrend.PARAM_COLUMN_WEBSITE.getParamName();
			orderedParameterMap.put(paramName, paramValues_website);
			if (0 == itemCnt) {
				mainItemArr = paramValues_website[0].split(PARAM_VALUES_SEPARATOR);
				for (int i = 0; i < mainItemArr.length; i++) {
					String mainValue = mainItemArr[i];
					// mainItemArr[i] = this.getWebsiteNameById(strTableName, mainValue);
					mainItemArr[i] = this.getWebsiteNameById(mainValue);
				}
			} else if (1 == itemCnt) {
				secItemArr = paramValues_website[0].split(PARAM_VALUES_SEPARATOR);
				for (int i = 0; i < secItemArr.length; i++) {
					String mainValue = secItemArr[i];
					// secItemArr[i] = this.getWebsiteNameById(strTableName, mainValue);
					secItemArr[i] = this.getWebsiteNameById(mainValue);
				}
			}
			itemCnt++;
		}
		if (paramValues_channel != null) {
			String paramName = EnumTrend.PARAM_COLUMN_CHANNEL.getParamName();
			orderedParameterMap.put(paramName, paramValues_channel);
			if (0 == itemCnt) {
				mainItemArr = paramValues_channel[0].split(PARAM_VALUES_SEPARATOR);
				for (int i = 0; i < mainItemArr.length; i++) {
					String mainValue = mainItemArr[i];
					// mainItemArr[i] = this.getChannelNameById(strTableName, mainValue);
					mainItemArr[i] = this.getChannelNameById(mainValue);
				}
			} else if (1 == itemCnt) {
				secItemArr = paramValues_channel[0].split(PARAM_VALUES_SEPARATOR);
				for (int i = 0; i < secItemArr.length; i++) {
					String mainValue = secItemArr[i];
					// secItemArr[i] = this.getChannelNameById(strTableName, mainValue);
					secItemArr[i] = this.getChannelNameById(mainValue);
				}
			}
			itemCnt++;
		}
		if (paramValues_features != null) {
			String paramName = EnumTrend.PARAM_COLUMN_FEATURES.getParamName();
			orderedParameterMap.put(paramName, paramValues_features);
			if (0 == itemCnt) {
				mainItemArr = paramValues_features[0].split(PARAM_VALUES_SEPARATOR);
			} else if (1 == itemCnt) {
				secItemArr = paramValues_features[0].split(PARAM_VALUES_SEPARATOR);
			}
			itemCnt++;
		}
		
		if (mainItemArr != null) {
			for (String mainItem : mainItemArr) {
				if (secItemArr != null) {
					for (String secItem : secItemArr) {
						itemNameList.add(mainItem + "-" + secItem);
					}
				} else {
					itemNameList.add(mainItem);
				}
			}
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