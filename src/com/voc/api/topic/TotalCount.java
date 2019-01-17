package com.voc.api.topic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.catalina.util.ParameterMap;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voc.api.RootAPI;
import com.voc.common.ApiResponse;
import com.voc.common.Common;
import com.voc.common.DBUtil;
import com.voc.enums.topic.EnumTotalCount;

/**
 * 查詢自訂主題分析口碑總數: 查詢時間區間內各項目獨立分析或交叉分析的口碑總數及比例
 * /topic/total-count.jsp
 * 
 * EX-1:
 * - URL: 
 *   ==>http://localhost:8080/voc/topic/total-count.jsp?user=tsmc&project_name=自訂汽車專案&topic=歐系汽車;日系汽車&website=台灣新浪網;PTT&start_date=2018-12-01&end_date=2018-12-31&limit=5
 * - SQL:
 *   ==>SELECT topic ,website_name ,SUM(reputation) AS count FROM ibuzz_voc.topic_reputation 
 *      WHERE user in ('tsmc') AND project_name in ('自訂汽車專案') AND topic in ('歐系汽車','日系汽車') AND website_name in ('台灣新浪網','PTT') 
 *      AND DATE_FORMAT(date, '%Y-%m-%d') >= '2018-12-01' AND DATE_FORMAT(date, '%Y-%m-%d') <= '2018-12-31' 
 *      GROUP BY topic, website_name ORDER BY count DESC LIMIT 5
 *      
 * EX-2:
 * - URL: 
 *   ==>http://localhost:8080/voc/topic/total-count.jsp?user=tsmc&project_name=自訂汽車專案&topic=歐系汽車;日系汽車&start_date=2018-12-01&end_date=2018-12-31
 * - SQL:
 *   ==>SELECT topic ,SUM(reputation) AS count FROM ibuzz_voc.topic_reputation 
 *      WHERE user in ('tsmc') AND project_name in ('自訂汽車專案') AND topic in ('歐系汽車','日系汽車') 
 *      AND DATE_FORMAT(date, '%Y-%m-%d') >= '2018-12-01' AND DATE_FORMAT(date, '%Y-%m-%d') <= '2018-12-31' 
 *      GROUP BY topic ORDER BY count DESC LIMIT 10
 * 
 */
public class TotalCount extends RootAPI {
	private static final Logger LOGGER = LoggerFactory.getLogger(TotalCount.class);
	private Map<String, String[]> orderedParameterMap = new ParameterMap<>();
	private String selectUpdateTimeSQL;
	private List<String> itemNameList = new ArrayList<>();
	private String tableName;
	private int limit = 10; // Default: 10
	
	@Override
	public String processRequest(HttpServletRequest request) {
		JSONObject errorResponse = this.validateAndSetOrderedParameterMap(request);
		if (errorResponse != null) {
			return errorResponse.toString();
		}
		JSONArray itemArray = this.queryData();
		String update_time = "";
		if (itemArray != null) {
			if (itemArray.length() > 0) {
				update_time = this.queryUpdateTime(this.selectUpdateTimeSQL);
			}
			JSONObject successObject = ApiResponse.successTemplate();
			successObject.put("update_time", update_time);
			successObject.put("result", itemArray);
			LOGGER.info("responseJsonStr=" + successObject.toString());
			return successObject.toString();
		}
		return ApiResponse.unknownError().toString();
	}
	
	private JSONArray queryData() {
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		StringBuffer selectSQL = new StringBuffer();
		try {
			selectSQL.append(this.genSelectClause());
			selectSQL.append(this.genWhereClause());
			selectSQL.append(this.genGroupByOrderByClause());
			selectSQL.append("LIMIT ? ");
			// LOGGER.debug("selectSQL=" + selectSQL.toString());
			
			conn = DBUtil.getConn();
			preparedStatement = conn.prepareStatement(selectSQL.toString());
			this.setWhereClauseValues(preparedStatement);
			
			String psSQLStr = preparedStatement.toString();
			LOGGER.debug("psSQLStr = " + psSQLStr);
			this.selectUpdateTimeSQL = "SELECT MAX(DATE_FORMAT(update_time, '%Y-%m-%d %H:%i:%s')) AS " + UPDATE_TIME + psSQLStr.substring(psSQLStr.indexOf(" FROM "), psSQLStr.indexOf(" GROUP BY "));
			LOGGER.debug("selectUpdateTimeSQL = " + this.selectUpdateTimeSQL);
			
			JSONArray itemArray = new JSONArray();
			Map<String, Integer> hash_itemName_count = new HashMap<>();
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				StringBuffer item = new StringBuffer();
				int i = 0;
				for (Map.Entry<String, String[]> entry : this.orderedParameterMap.entrySet()) {
					String paramName = entry.getKey();
					if ("user".equals(paramName) || "project_name".equals(paramName)) {
						continue;
					}
					String columnName = this.getColumnName(paramName);
					if ("channel_id".equals(columnName)) {
						columnName = "channel_name";
					}
					if (!"date".equals(columnName)) {
						String s = rs.getString(columnName);;
						if (i == 0) {
							item.append(s);
						} else {
							item.append("-").append(s);
						}
					}
					i++;
				}
				int count = rs.getInt("count");
				LOGGER.debug("item=" + item.toString() + ", count=" + count);
				hash_itemName_count.put(item.toString(), count);
				
				JSONObject itemObject = new JSONObject();
				itemObject.put("item", item.toString());
				itemObject.put("count", count);
				itemArray.put(itemObject);
			}
			LOGGER.debug("hash_itemName_count=" + hash_itemName_count);
			

			int desc_remainingCnt = this.limit - itemArray.length();
			if (desc_remainingCnt > 0) {
				for (String itemName: itemNameList) {
					Integer count = hash_itemName_count.get(itemName);
					if (count == null) {
						if (desc_remainingCnt > 0) {
							JSONObject itemObject = new JSONObject();
							itemObject.put("item", itemName);
							itemObject.put("count", 0);
							itemArray.put(itemObject);
							desc_remainingCnt--;
						}
					}
				}
			}
			return itemArray;
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, preparedStatement, conn);
		}
		return null;
	}

	private String[] trimValues(String[] values) {
		StringBuffer trimedValuesSB = new StringBuffer();
		String[] vArr = StringUtils.trimToEmpty(values[0]).split(PARAM_VALUES_SEPARATOR);
		int i = 0;
		for (String v : vArr) {
			if (i == 0) {
				trimedValuesSB.append(StringUtils.trimToEmpty(v));
			} else {
				trimedValuesSB.append(PARAM_VALUES_SEPARATOR).append(StringUtils.trimToEmpty(v));
			}
			i++;
		}
		
		String[] trimedValues = new String[] {
				trimedValuesSB.toString()
		};
		return trimedValues;
	}
	
	private JSONObject checkDateParameters(Map<String, String[]> parameterMap) {
		String param_startDate = EnumTotalCount.PARAM_COLUMN_START_DATE.getParamName(); // start_date
		String param_endDate = EnumTotalCount.PARAM_COLUMN_END_DATE.getParamName(); // end_date
		if (!parameterMap.containsKey(param_startDate) || !parameterMap.containsKey(param_endDate)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}
		
		String value_startDate = StringUtils.trimToEmpty(parameterMap.get(param_startDate)[0]);
		String value_endDate = StringUtils.trimToEmpty(parameterMap.get(param_endDate)[0]);
		if (!Common.isValidDate(value_startDate, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid start_date.");
		}
		if (!Common.isValidDate(value_endDate, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid end_date.");
		}
		if (!Common.isValidStartDate(value_startDate, value_endDate, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid period values.");
		}
		return null;
	}
	
	private JSONObject validateAndSetOrderedParameterMap(HttpServletRequest request) {
		String user = request.getParameter("user");
		String project_name = request.getParameter("project_name");
		if (StringUtils.isBlank(user) || StringUtils.isBlank(project_name)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}
		
		Map<String, String[]> parameterMap = request.getParameterMap();
		JSONObject errorResponse = this.checkDateParameters(parameterMap);
		if (errorResponse != null) {
			return errorResponse;
		}
		this.tableName = TABLE_TOPIC_REPUTATION;
		
		String[] paramValues_user = null;
		String[] paramValues_projectName = null;
		String[] paramValues_topic = null;
		String[] paramValues_source = null;
		String[] paramValues_website = null;
		String[] paramValues_channel = null;
		String[] paramValues_sentiment = null;
		String[] paramValues_startDate = null;
		String[] paramValues_endDate = null;
		
		String limitStr = StringUtils.trimToEmpty(request.getParameter("limit"));
		if (!StringUtils.isEmpty(limitStr)) {
			try {
				this.limit = Integer.parseInt(limitStr);
			} catch (Exception e) {
				LOGGER.error(e.getMessage());
			}
		}
		
		for (Map.Entry<String, String[]> entry : parameterMap.entrySet()) {
			String paramName = entry.getKey();
			if (API_KEY.equals(paramName)) continue;
			
			EnumTotalCount enumTotalCount = EnumTotalCount.getEnum(paramName);
			if (enumTotalCount == null) {
				return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Unknown parameter: param=" + paramName);
			}
			
			String[] values = entry.getValue();
			if (StringUtils.isBlank(values[0])) {
				continue;
			}
			String[] trimedValues = this.trimValues(values);
			
			switch (enumTotalCount) {
			case PARAM_COLUMN_USER:
				paramValues_user = trimedValues;
				break;
			case PARAM_COLUMN_PROJECT_NAME:
				paramValues_projectName = trimedValues;
				break;
			case PARAM_COLUMN_TOPIC:
				paramValues_topic = trimedValues;
				break;
			case PARAM_COLUMN_SOURCE:
				paramValues_source = trimedValues;
				break;
			case PARAM_COLUMN_WEBSITE:
				paramValues_website = trimedValues;
				break;
			case PARAM_COLUMN_CHANNEL:
				paramValues_channel = trimedValues;
				break;
			case PARAM_COLUMN_SENTIMENT:
				paramValues_sentiment = trimedValues;
				break;
			case PARAM_COLUMN_START_DATE:
				paramValues_startDate = trimedValues;
				paramValues_startDate[0] = Common.formatDate(paramValues_startDate[0], "yyyy-MM-dd");
				break;
			case PARAM_COLUMN_END_DATE:
				paramValues_endDate = trimedValues;
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
		if (paramValues_user != null) {
			String paramName = EnumTotalCount.PARAM_COLUMN_USER.getParamName();
			this.orderedParameterMap.put(paramName, paramValues_user);
//			if (itemCnt == 0) {
//				mainItemArr = paramValues_user[0].split(PARAM_VALUES_SEPARATOR);
//			} else if (itemCnt == 1) {
//				secItemArr = paramValues_user[0].split(PARAM_VALUES_SEPARATOR);
//			}
//			itemCnt++;
		}
		if (paramValues_projectName != null) {
			String paramName = EnumTotalCount.PARAM_COLUMN_PROJECT_NAME.getParamName();
			this.orderedParameterMap.put(paramName, paramValues_projectName);
//			if (itemCnt == 0) {
//				mainItemArr = paramValues_projectName[0].split(PARAM_VALUES_SEPARATOR);
//			} else if (itemCnt == 1) {
//				secItemArr = paramValues_projectName[0].split(PARAM_VALUES_SEPARATOR);
//			}
//			itemCnt++;
		}
		if (paramValues_topic != null) {
			String paramName = EnumTotalCount.PARAM_COLUMN_TOPIC.getParamName();
			this.orderedParameterMap.put(paramName, paramValues_topic);
			if (itemCnt == 0) {
				mainItemArr = paramValues_topic[0].split(PARAM_VALUES_SEPARATOR);
			} else if (itemCnt == 1) {
				secItemArr = paramValues_topic[0].split(PARAM_VALUES_SEPARATOR);
			}
			itemCnt++;
		}
		if (paramValues_source != null) {
			String paramName = EnumTotalCount.PARAM_COLUMN_SOURCE.getParamName();
			this.orderedParameterMap.put(paramName, paramValues_source);
			if (itemCnt == 0) {
				mainItemArr = paramValues_source[0].split(PARAM_VALUES_SEPARATOR);
			} else if (itemCnt == 1) {
				secItemArr = paramValues_source[0].split(PARAM_VALUES_SEPARATOR);
			}
			itemCnt++;
		}
		if (paramValues_website != null) {
			String paramName = EnumTotalCount.PARAM_COLUMN_WEBSITE.getParamName();
			this.orderedParameterMap.put(paramName, paramValues_website);
			if (itemCnt == 0) {
				mainItemArr = paramValues_website[0].split(PARAM_VALUES_SEPARATOR);
//				for (int i = 0; i < mainItemArr.length; i++) {
//					String mainValue = mainItemArr[i];
//					// mainItemArr[i] = this.getWebsiteNameById(this.tableName, mainValue);
//					mainItemArr[i] = this.getWebsiteNameById(mainValue);
//				}
			} else if (itemCnt == 1) {
				secItemArr = paramValues_website[0].split(PARAM_VALUES_SEPARATOR);
//				for (int i = 0; i < secItemArr.length; i++) {
//					String mainValue = secItemArr[i];
//					// secItemArr[i] = this.getWebsiteNameById(this.tableName, mainValue);
//					secItemArr[i] = this.getWebsiteNameById(mainValue);
//				}
			}
			itemCnt++;
		}
		if (paramValues_channel != null) {
			String paramName = EnumTotalCount.PARAM_COLUMN_CHANNEL.getParamName();
			this.orderedParameterMap.put(paramName, paramValues_channel);
			if (itemCnt == 0) {
				mainItemArr = paramValues_channel[0].split(PARAM_VALUES_SEPARATOR);
				for (int i = 0; i < mainItemArr.length; i++) {
					String mainValue = mainItemArr[i];
					// mainItemArr[i] = this.getChannelNameById(this.tableName, mainValue);
					mainItemArr[i] = this.getChannelNameById(mainValue);
				}
			} else if (itemCnt == 1) {
				secItemArr = paramValues_channel[0].split(PARAM_VALUES_SEPARATOR);
				for (int i = 0; i < secItemArr.length; i++) {
					String secValue = secItemArr[i];
					// secItemArr[i] = this.getChannelNameById(this.tableName, secValue);
					secItemArr[i] = this.getChannelNameById(secValue);
				}
			}
			itemCnt++;
		}
		if (paramValues_sentiment != null) {
			String paramName = EnumTotalCount.PARAM_COLUMN_SENTIMENT.getParamName();
			this.orderedParameterMap.put(paramName, paramValues_sentiment);
			if (itemCnt == 0) {
				mainItemArr = paramValues_sentiment[0].split(PARAM_VALUES_SEPARATOR);
			} else if (itemCnt == 1) {
				secItemArr = paramValues_sentiment[0].split(PARAM_VALUES_SEPARATOR);
			}
			// itemCnt++;
		}
		
		// topic, source, website, channel 至少須帶其中一個參數
		if (itemCnt == 0) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
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
			String paramName = EnumTotalCount.PARAM_COLUMN_START_DATE.getParamName();
			this.orderedParameterMap.put(paramName, paramValues_startDate);
		}
		if (paramValues_endDate != null) {
			String paramName = EnumTotalCount.PARAM_COLUMN_END_DATE.getParamName();
			this.orderedParameterMap.put(paramName, paramValues_endDate);
		}
		return null;
	}
	
	private String genSelectClause() {
		StringBuffer selectClauseSB = new StringBuffer();
		selectClauseSB.append("SELECT ");
		int i = 0;
		for (Map.Entry<String, String[]> entry : this.orderedParameterMap.entrySet()) {
			String paramName = entry.getKey();
			if ("user".equals(paramName) || "project_name".equals(paramName)) {
				continue;
			}
			if (this.isItemParamName(paramName)) {
				String columnName = this.getColumnName(paramName);
				if ("website_id".equals(columnName)) {
					columnName = "website_name";
				} else if ("channel_id".equals(columnName)) {
					columnName = "channel_name";
				}
				if (i == 0) {
					selectClauseSB.append(columnName);
				} else {
					selectClauseSB.append(" ,").append(columnName);
				}
				i++;
			}
		}
		selectClauseSB.append(" ,").append("SUM(reputation) AS count FROM ").append(this.tableName).append(" ");
		return selectClauseSB.toString();
	}
	
	private String genWhereClause() {
		StringBuffer whereClauseSB = new StringBuffer();
		int i = 0;
		for (Map.Entry<String, String[]> entry : this.orderedParameterMap.entrySet()) {
			String paramName = entry.getKey();			
			String columnName = this.getColumnName(paramName);
			if (i == 0) {
				whereClauseSB.append("WHERE ");
			} else {
				whereClauseSB.append("AND ");
			}
			if (paramName.equals("start_date")) {
				whereClauseSB.append("DATE_FORMAT(date, '%Y-%m-%d') >= ? ");
			} else if (paramName.equals("end_date")) {
				whereClauseSB.append("DATE_FORMAT(date, '%Y-%m-%d') <= ? ");
			} else {
				whereClauseSB.append(columnName);
				whereClauseSB.append(" in (");
				String[] valueArr = entry.getValue()[0].split(PARAM_VALUES_SEPARATOR);
				for (int cnt = 0; cnt < valueArr.length; cnt++) {
					if (cnt == 0) {
						whereClauseSB.append("?");
					} else {
						whereClauseSB.append(",?");
					}
				}
				whereClauseSB.append(") ");
			}
			i++;
		}
		return whereClauseSB.toString();
	}
	
	private String genGroupByOrderByClause() {
		StringBuffer groupByOrderByClauseSB = new StringBuffer();
		StringBuffer columnsSB = new StringBuffer();
		int i = 0;
		for (Map.Entry<String, String[]> entry : this.orderedParameterMap.entrySet()) {
			String paramName = entry.getKey();
			if ("user".equals(paramName) || "project_name".equals(paramName)) {
				continue;
			}
			String columnName = this.getColumnName(paramName);
			if (!"date".equals(columnName)) {
				if (i == 0) {
					columnsSB.append(columnName);
				} else {
					columnsSB.append(", ").append(columnName);
				}
			}
			i++;
		}
		groupByOrderByClauseSB.append("GROUP BY ").append(columnsSB.toString()).append(" ");
		groupByOrderByClauseSB.append("ORDER BY count DESC ");
		return groupByOrderByClauseSB.toString();
	}

	private void setWhereClauseValues(PreparedStatement preparedStatement) throws Exception {
		int i = 0;
		for (Map.Entry<String, String[]> entry : this.orderedParameterMap.entrySet()) {
			String paramName = entry.getKey();
			String[] values = entry.getValue();
			String value = "";
			if (values != null && values.length > 0) {
				value = values[0];
				if (paramName.equals("start_date") || paramName.equals("end_date")) {
					int parameterIndex = i + 1;
					preparedStatement.setObject(parameterIndex, value);
					// LOGGER.debug(parameterIndex + ":" + value);
					i++;
				} else {
					String[] valueArr = entry.getValue()[0].split(PARAM_VALUES_SEPARATOR);
					for (String v : valueArr) {
						int parameterIndex = i + 1;
						preparedStatement.setObject(parameterIndex, v);
						// LOGGER.debug(parameterIndex + ":" + v);
						i++;
					}
				}
			}
		}
		int parameterIndex = i + 1;
		preparedStatement.setObject(parameterIndex, this.limit);
	}

}
