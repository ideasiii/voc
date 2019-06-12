package com.voc.api.topic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
import com.voc.common.ApiUtil;
import com.voc.common.Common;
import com.voc.common.DBUtil;
import com.voc.enums.EnumSentiment;
import com.voc.enums.topic.EnumTrend;

/**
 * Ex:
 * http://localhost:8080/voc/topic/trend.jsp?user=tsmc&project_name=自訂汽車專案&topic=日系汽車;歐系汽車&website=壹蘋果網絡;PIXNET痞客邦&start_date=2018-12-01&end_date=2018-12-17
 * http://localhost:8080/voc/topic/trend.jsp?user=tsmc&project_name=自訂汽車專案&topic=日系汽車;歐系汽車&website=壹蘋果網絡;PIXNET痞客邦&start_date=2018-12-01&end_date=2018-12-17&limit=3&interval=monthly
 * 
 */

public class Trend extends RootAPI {
	private static final Logger LOGGER = LoggerFactory.getLogger(Trend.class);
	Map<String, String[]> orderedParameterMap = new ParameterMap<>();
	private List<String> itemNameList = new ArrayList<>();
	private JSONArray sortedJsonArray;
	private String selectUpdateTimeSQL;
	private String strTableName = TABLE_TOPIC_REPUTATION;
	private String strInterval = Common.INTERVAL_DAILY;
	private int limit = 10; // Default: 10
	private String strStartDate;
	private String strEndDate;

	@Override
	public String processRequest(HttpServletRequest request) {

		Map<String, String[]> paramMap = request.getParameterMap();

		if (!hasRequiredParameters(paramMap)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER).toString();
		}

		if (hasInterval(paramMap)) {
			strInterval = request.getParameter("interval");
			if (!Common.isValidInterval(strInterval)) {
				return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid interval.").toString();
			}
		}

		JSONObject errorResponse = adjustParameterOrder(request);
		if (null != errorResponse) {
			return errorResponse.toString();
		}

		String limitStr = StringUtils.trimToEmpty(request.getParameter("limit"));
		if (!StringUtils.isEmpty(limitStr)) {
			try {
				this.limit = Integer.parseInt(limitStr);
			} catch (Exception e) {
				LOGGER.error(e.getMessage());
			}
		}

		JSONObject jobj = new JSONObject();
		JSONArray resArray = new JSONArray();
		boolean querySuccess = query(orderedParameterMap, resArray);
		LOGGER.info("resArray: " + resArray);
		String update_time = this.queryUpdateTime(this.selectUpdateTimeSQL);

		if (querySuccess) {
			jobj = ApiResponse.successTemplate();
			jobj.put("update_time", update_time);
			// jobj.put("result", resArray);
			jobj.put("result", this.sortedJsonArray);
			LOGGER.info("response: " + jobj.toString());
		} else {
			jobj = ApiResponse.unknownError();
		}
		return jobj.toString();
	}

	private boolean hasRequiredParameters(Map<String, String[]> paramMap) {
		return paramMap.containsKey("user") && paramMap.containsKey("project_name")
				&& paramMap.containsKey("start_date") && paramMap.containsKey("end_date");
	}

	private boolean hasInterval(Map<String, String[]> paramMap) {
		return paramMap.containsKey("interval");
	}

	private JSONObject dateValidate(Map<String, String[]> paramMap) {
		String sd = paramMap.get("start_date")[0];
		String ed = paramMap.get("end_date")[0];

		if (!Common.isValidDate(sd, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid start_date.");
		}

		if (!Common.isValidDate(ed, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid end_date.");
		}

		if (!Common.isValidStartDate(sd, ed, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid period values.");
		}
		return null;
	}

	private boolean query(Map<String, String[]> paramMap, final JSONArray out) {
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		String date = null;

		StringBuffer querySQL = new StringBuffer();
		try {
			querySQL.append(genSelectClause());
			querySQL.append(genWhereClause());
			querySQL.append(genGroupByClause());

			LOGGER.info("querySQL: " + querySQL.toString());

			conn = DBUtil.getConn();
			pst = conn.prepareStatement(querySQL.toString());
			setWhereClauseValues(pst);

			// get update_time SQL
			String strPstSQL = pst.toString();
			LOGGER.info("strPstSQL: " + strPstSQL);
			this.selectUpdateTimeSQL = "SELECT MAX(DATE_FORMAT(update_time, '%Y-%m-%d %H:%i:%s')) AS " + UPDATE_TIME
					+ strPstSQL.substring(strPstSQL.indexOf(" FROM "), strPstSQL.indexOf(" GROUP BY "));
			LOGGER.info("selectUpdateTimeSQL: " + this.selectUpdateTimeSQL);

			Map<String, Map<String, Integer>> hash_itemName_countMap = new HashMap<>();
			Map<String, Map<String, Integer>> hash_itemName_titleMap = new HashMap<>();
			Map<String, Map<String, Integer>> hash_itemName_contentMap = new HashMap<>();
			Map<String, Map<String, Integer>> hash_itemName_commentMap = new HashMap<>();
			Map<String, Integer> hash_itemName_count = new HashMap<String, Integer>();
			Map<String, Integer> hash_itemName_title = new HashMap<String, Integer>();
			Map<String, Integer> hash_itemName_content = new HashMap<String, Integer>();
			Map<String, Integer> hash_itemName_comment = new HashMap<String, Integer>();
			
			rs = pst.executeQuery();
			while (rs.next()) {
				StringBuffer item = new StringBuffer();
				int i = 0;
				for (Map.Entry<String, String[]> entry : orderedParameterMap.entrySet()) {
					String paramName = entry.getKey();
					if (paramName.equals("user") || paramName.equals("project_name")) {
						continue;
					}
					String columnName = getColumnName(paramName);
					if ("website_id".equals(columnName)) {
						columnName = "website_name";
					} else if ("channel_id".equals(columnName)) {
						columnName = "channel_display_name";
					}
					if (!"rep_date".equals(columnName)) {
						String str = rs.getString(columnName);
						if (paramName.equals("sentiment")) {
							str = EnumSentiment.getEnum(str).getName();
						}
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
				int title_count = rs.getInt("title_count");
				int content_count = rs.getInt("content_count");
				int comment_count = rs.getInt("comment_count");
				LOGGER.info("item: " + item.toString() + ", date: " + date + ", count: " + count + ", title_count: " + title_count + ", content_count: " + content_count + ", comment_count: " + comment_count);

				if (hash_itemName_countMap.get(item.toString()) == null) {
					hash_itemName_countMap.put(item.toString(), new HashMap<String, Integer>());
				}
				if (hash_itemName_titleMap.get(item.toString()) == null) {
					hash_itemName_titleMap.put(item.toString(), new HashMap<String, Integer>());
				}
				if (hash_itemName_contentMap.get(item.toString()) == null) {
					hash_itemName_contentMap.put(item.toString(), new HashMap<String, Integer>());
				}
				if (hash_itemName_commentMap.get(item.toString()) == null) {
					hash_itemName_commentMap.put(item.toString(), new HashMap<String, Integer>());
				}
				hash_itemName_count = hash_itemName_countMap.get(item.toString());
				hash_itemName_count.put(date, count);
				hash_itemName_title = hash_itemName_titleMap.get(item.toString());
				hash_itemName_title.put(date, title_count);
				hash_itemName_content = hash_itemName_contentMap.get(item.toString());
				hash_itemName_content.put(date, content_count);
				hash_itemName_comment = hash_itemName_commentMap.get(item.toString());
				hash_itemName_comment.put(date, comment_count);
			}

			for (String itemName : itemNameList) {
				hash_itemName_count = hash_itemName_countMap.get(itemName);
				hash_itemName_title = hash_itemName_titleMap.get(itemName);
				hash_itemName_content = hash_itemName_contentMap.get(itemName);
				hash_itemName_comment = hash_itemName_commentMap.get(itemName);
				
				JSONArray dataArray = new JSONArray();
				List<String> dateList = null;
				if (strInterval.equals(Common.INTERVAL_MONTHLY)) {
					dateList = ApiUtil.getMonthlyList(strStartDate, strEndDate);
				} else {
					dateList = ApiUtil.getDailyList(strStartDate, strEndDate);
				}
				for (String dataStr : dateList) {
					Integer count = null;
					Integer title_count = null;
					Integer content_count = null;
					Integer comment_count = null;
					if (null != hash_itemName_count) {
						count = hash_itemName_count.get(dataStr);
					}
					if (count == null)
						count = 0;
					if (null != hash_itemName_title) {
						title_count = hash_itemName_title.get(dataStr);
					}
					if (title_count == null)
						title_count = 0;
					if (null != hash_itemName_content) {
						content_count = hash_itemName_content.get(dataStr);
					}
					if (content_count == null)
						content_count = 0;
					if (null != hash_itemName_comment) {
						comment_count = hash_itemName_comment.get(dataStr);
					}
					if (comment_count == null)
						comment_count = 0;
					
					JSONObject dataObject = new JSONObject();
					dataObject.put("date", dataStr);
					dataObject.put("count", count);
					dataObject.put("title_count", title_count);
					dataObject.put("content_count", content_count);
					dataObject.put("comment_count", comment_count);
					dataArray.put(dataObject);
				}
				JSONObject resultObj = new JSONObject();
				resultObj.put("item", itemName);
				resultObj.put("data", dataArray);
				resultObj.put("totalCount", this.getTotalCount(dataArray)); // for sort later
				out.put(resultObj);
			}
			this.sortedJsonArray = this.getSortedResultArray(out, this.limit);
			return true;

		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, pst, conn);
		}
		return false;
	}

	private String genSelectClause() {

		StringBuffer sql = new StringBuffer();
		sql.append("SELECT ");
		int i = 0;
		for (Map.Entry<String, String[]> entry : orderedParameterMap.entrySet()) {
			String paramName = entry.getKey();
			if (paramName.equals("user") || paramName.equals("project_name")) {
				continue;
			}
			if (isItemParamName(paramName)) {
				String columnName = this.getColumnName(paramName);
				if ("website_id".equals(columnName)) {
					columnName = "website_name";
				} else if ("channel_id".equals(columnName)) {
					columnName = "channel_display_name";
				}
				if (0 == i) {
					sql.append(columnName);
				} else {
					sql.append(" ,").append(columnName);
				}
				i++;
			}
		}
		if (strInterval.equals(Common.INTERVAL_DAILY)) {
			sql.append(" ,").append("DATE_FORMAT(rep_date, '%Y-%m-%d') AS dailyStr");
		} else if (strInterval.equals(Common.INTERVAL_MONTHLY)) {
			sql.append(" ,").append("DATE_FORMAT(rep_date, '%Y-%m') AS monthlyStr");
		}
		sql.append(" ,").append(
				"SUM(reputation) AS count, SUM(title_hit) AS title_count, SUM(content_hit) AS content_count, SUM(comment_hit) AS comment_count FROM ")
				.append(strTableName).append(" ");

		return sql.toString();
	}

	private String genWhereClause() {

		StringBuffer sql = new StringBuffer();
		int i = 0;
		for (Map.Entry<String, String[]> entry : orderedParameterMap.entrySet()) {
			String paramName = entry.getKey();
			String columnName = this.getColumnName(paramName);

			if (0 == i) {
				sql.append("WHERE ");
			} else {
				sql.append("AND ");
			}

			if (paramName.equals("start_date")) {
				sql.append(" DATE_FORMAT(rep_date, '%Y-%m-%d') >= ? ");
			} else if (paramName.equals("end_date")) {
				sql.append(" DATE_FORMAT(rep_date, '%Y-%m-%d') <= ? ");
			} else {
				sql.append("`" + columnName + "`");
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

	private String genGroupByClause() {
		StringBuffer sql = new StringBuffer();
		StringBuffer groupByColumns = new StringBuffer();
		int i = 0;
		for (Map.Entry<String, String[]> entry : orderedParameterMap.entrySet()) {
			String paramName = entry.getKey();
			if (paramName.equals("user") || paramName.equals("project_name")) {
				continue;
			}
			String columnName = this.getColumnName(paramName);
			if (!"rep_date".equals(columnName)) {
				if (0 == i) {
					groupByColumns.append(columnName);
				} else {
					groupByColumns.append(", ").append(columnName);
				}
			}
			i++;
		}

		if (strInterval.equals(Common.INTERVAL_MONTHLY)) {
			sql.append(" GROUP BY DATE_FORMAT(rep_date, '%Y-%m'), ").append(groupByColumns.toString());
		} else if (strInterval.equals(Common.INTERVAL_DAILY)) {
			sql.append(" GROUP BY DATE_FORMAT(rep_date, '%Y-%m-%d'), ").append(groupByColumns.toString());
		}
		return sql.toString();
	}

	private void setWhereClauseValues(PreparedStatement pst) throws Exception {

		int i = 0;
		for (Map.Entry<String, String[]> entry : orderedParameterMap.entrySet()) {
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

	private Integer getTotalCount(JSONArray dataArray) {
		Integer totalCount = 0;
		for (int i = 0; i < dataArray.length(); i++) {
			JSONObject dataObject = dataArray.getJSONObject(i);
			totalCount += dataObject.getInt("count");
		}
		return totalCount;
	}

	private JSONArray getSortedResultArray(JSONArray resultArray, int limit) {
		JSONArray sortedJsonArray = new JSONArray();

		// Step_1: parse your array in a list
		List<JSONObject> jsonList = new ArrayList<JSONObject>();
		for (int i = 0; i < resultArray.length(); i++) {
			jsonList.add(resultArray.getJSONObject(i));
		}

		// Step_2: then use collection.sort to sort the newly created list
		Collections.sort(jsonList, new Comparator<JSONObject>() {
			public int compare(JSONObject a, JSONObject b) {
				Integer valA = 0;
				Integer valB = 0;
				try {
					valA = (Integer) a.get("totalCount");
					valB = (Integer) b.get("totalCount");
				} catch (Exception e) {
					e.printStackTrace();
				}
				return valA.compareTo(valB) * -1;
			}
		});

		// Handle for limit:
		int listSize = jsonList.size();
		int recordSize = limit;
		if (limit > listSize) {
			recordSize = listSize;
		}
		jsonList = jsonList.subList(0, recordSize);

		// Step_3: Insert the sorted values in your array
		for (int i = 0; i < jsonList.size(); i++) {
			JSONObject jsonObject = jsonList.get(i);
			jsonObject.remove("totalCount"); // totalCount 不用顯示: after sorting, remove it.
			sortedJsonArray.put(jsonObject);
		}

		return sortedJsonArray;
	}

	private JSONObject adjustParameterOrder(HttpServletRequest request) {
		Map<String, String[]> parameterMap = request.getParameterMap();
		final String strUser = request.getParameter("user");
		final String strProjectName = request.getParameter("project_name");
		strStartDate = request.getParameter("start_date");
		strEndDate = request.getParameter("end_date");

		if (StringUtils.isBlank(strUser) || StringUtils.isBlank(strProjectName) || StringUtils.isBlank(strStartDate)
				|| StringUtils.isBlank(strEndDate)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}

		JSONObject errorResponse = dateValidate(parameterMap);
		if (null != errorResponse) {
			return errorResponse;
		}

		String[] paramValues_user = null;
		String[] paramValues_topic = null;
		String[] paramValues_projectName = null;
		String[] paramValues_mediaType = null;
		String[] paramValues_website = null;
		String[] paramValues_channel = null;
		String[] paramValues_sentiment = null;
		String[] paramValues_startDate = null;
		String[] paramValues_endDate = null;

		for (Map.Entry<String, String[]> entry : parameterMap.entrySet()) {
			String paramName = entry.getKey();
			if (API_KEY.equals(paramName))
				continue;

			EnumTrend enumTrend = EnumTrend.getEnum(paramName);
			if (null == enumTrend)
				continue;

			String[] values = entry.getValue();
			if (StringUtils.isBlank(values[0])) {
				continue;
			}

			switch (enumTrend) {
			case PARAM_COLUMN_USER:
				paramValues_user = values;
				break;
			case PARAM_COLUMN_TOPIC:
				paramValues_topic = values;
				break;
			case PARAM_COLUMN_PROJECT:
				paramValues_projectName = values;
				break;
			case PARAM_COLUMN_MEDIA_TYPE:
				paramValues_mediaType = values;
				break;
			case PARAM_COLUMN_WEBSITE:
				paramValues_website = values;
				break;
			case PARAM_COLUMN_CHANNEL:
				paramValues_channel = values;
				break;
			case PARAM_COLUMN_SENTIMENT:
				paramValues_sentiment = values;
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

		if (paramValues_user != null) {
			String paramName = EnumTrend.PARAM_COLUMN_USER.getParamName();
			orderedParameterMap.put(paramName, paramValues_user);
		}
		if (paramValues_projectName != null) {
			String paramName = EnumTrend.PARAM_COLUMN_PROJECT.getParamName();
			orderedParameterMap.put(paramName, paramValues_projectName);
		}
		if (paramValues_topic != null) {
			String paramName = EnumTrend.PARAM_COLUMN_TOPIC.getParamName();
			orderedParameterMap.put(paramName, paramValues_topic);
			if (0 == itemCnt) {
				mainItemArr = paramValues_topic[0].split(PARAM_VALUES_SEPARATOR);
			} else if (1 == itemCnt) {
				secItemArr = paramValues_topic[0].split(PARAM_VALUES_SEPARATOR);
			}
			itemCnt++;
		}
		if (paramValues_mediaType != null) {
			String paramName = EnumTrend.PARAM_COLUMN_MEDIA_TYPE.getParamName();
			orderedParameterMap.put(paramName, paramValues_mediaType);
			if (0 == itemCnt) {
				mainItemArr = paramValues_mediaType[0].split(PARAM_VALUES_SEPARATOR);
			} else if (1 == itemCnt) {
				secItemArr = paramValues_mediaType[0].split(PARAM_VALUES_SEPARATOR);
			}
			itemCnt++;
		}
		if (paramValues_website != null) {
			String paramName = EnumTrend.PARAM_COLUMN_WEBSITE.getParamName();
			orderedParameterMap.put(paramName, paramValues_website);
			if (0 == itemCnt) {
				mainItemArr = paramValues_website[0].split(PARAM_VALUES_SEPARATOR);
//				for (int i = 0; i < mainItemArr.length; i++) {
//					String mainValue = mainItemArr[i];
//					// mainItemArr[i] = this.getWebsiteNameById(strTableName, mainValue);
//					mainItemArr[i] = this.getWebsiteNameById(mainValue);
//				}
			} else if (1 == itemCnt) {
				secItemArr = paramValues_website[0].split(PARAM_VALUES_SEPARATOR);
//				for (int i = 0; i < secItemArr.length; i++) {
//					String mainValue = secItemArr[i];
//					// secItemArr[i] = this.getWebsiteNameById(strTableName, mainValue);
//					secItemArr[i] = this.getWebsiteNameById(mainValue);
//				}
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
		if (paramValues_sentiment != null) {
			String paramName = EnumTrend.PARAM_COLUMN_SENTIMENT.getParamName();
			orderedParameterMap.put(paramName, paramValues_sentiment);
			if (0 == itemCnt) {
				mainItemArr = paramValues_sentiment[0].split(PARAM_VALUES_SEPARATOR);
				for (int i = 0; i < mainItemArr.length; i++) {
					String mainValue = mainItemArr[i];
					mainItemArr[i] = EnumSentiment.getEnum(mainValue).getName();
				}
			} else if (1 == itemCnt) {
				secItemArr = paramValues_sentiment[0].split(PARAM_VALUES_SEPARATOR);
				for (int i = 0; i < secItemArr.length; i++) {
					String secValue = secItemArr[i];
					secItemArr[i] = EnumSentiment.getEnum(secValue).getName();
				}
			}
			itemCnt++;
		}

		if (0 == itemCnt) {
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
			String paramName = EnumTrend.PARAM_COLUMN_START_DATE.getParamName();
			orderedParameterMap.put(paramName, paramValues_startDate);
		}
		if (paramValues_endDate != null) {
			String paramName = EnumTrend.PARAM_COLUMN_END_DATE.getParamName();
			orderedParameterMap.put(paramName, paramValues_endDate);
		}
		return null;
	}

}
