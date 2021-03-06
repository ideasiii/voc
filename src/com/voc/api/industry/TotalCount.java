package com.voc.api.industry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
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
import com.voc.enums.EnumSentiment;
import com.voc.enums.industry.EnumTotalCount;

/**
 * 查詢產業分析口碑總數: /industry/total-count.jsp
 * 
 * EX-1: ibuzz_voc.product_reputation (產品表格) SELECT SUM(reputation) FROM
 * ibuzz_voc.product_reputation WHERE series = '掀背車' and website_id =
 * '5b29c821a85d0a7df5c3ff22' AND date >= '2018-05-01 00:00:00' AND date <=
 * '2018-05-30 23:59:59';
 * http://localhost:8080/voc/industry/total-count.jsp?series=掀背車&website=5b29c821a85d0a7df5c3ff22&start_date=2018-05-01
 * 00:00:00&end_date=2018-05-30 23:59:59
 * http://localhost:8080/voc/industry/total-count.jsp?series=掀背車&website=5b29c821a85d0a7df5c3ff22&start_date=2018-05-01&end_date=2018-05-30
 * 
 * SELECT brand, series, SUM(reputation) AS count FROM
 * ibuzz_voc.product_reputation where brand in ('BENZ', 'BMW') and series
 * in('掀背車', '轎車') and DATE_FORMAT(date, '%Y-%m-%d') >= '2018-01-01' AND
 * DATE_FORMAT(date, '%Y-%m-%d') <= '2018-10-31' GROUP BY brand, series;
 * http://localhost:8080/voc/industry/total-count.jsp?brand=BENZ;BMW&series=掀背車;轎車&start_date=2018-01-01&end_date=2018-10-31
 * 
 * 
 * EX-2: ibuzz_voc.brand_reputation (品牌表格) SELECT SUM(reputation) FROM
 * ibuzz_voc.brand_reputation where brand = 'BENZ' and website_id =
 * '5b29c824a85d0a7df5c40080' and date >= '2018-05-01 00:00:00' AND date <=
 * '2018-05-02 23:59:59';
 * http://localhost:8080/voc/industry/total-count.jsp?brand=BENZ&website=5b29c824a85d0a7df5c40080&start_date=2018-05-01
 * 00:00:00&end_date=2018-05-02 23:59:59
 * http://localhost:8080/voc/industry/total-count.jsp?brand=BENZ&website=5b29c824a85d0a7df5c40080&start_date=2018-05-01&end_date=2018-05-02
 * 
 * SELECT SUM(reputation) FROM ibuzz_voc.brand_reputation where brand in
 * ('BENZ', 'BMW') and website_id = '5b29c824a85d0a7df5c40080' and date >=
 * '2018-05-01 00:00:00' AND date <= '2018-05-02 23:59:59';
 * http://localhost:8080/voc/industry/total-count.jsp?brand=BENZ;BMW&website=5b29c824a85d0a7df5c40080&start_date=2018-05-01&end_date=2018-05-02
 * 
 * SELECT SUM(reputation) FROM ibuzz_voc.brand_reputation where brand in
 * ('BENZ', 'BMW') and website_id in ( '5b29c824a85d0a7df5c40080',
 * '5b29c821a85d0a7df5c3ff22') and date >= '2018-05-01 00:00:00' AND date <=
 * '2018-05-02 23:59:59';
 * http://localhost:8080/voc/industry/total-count.jsp?brand=BENZ;BMW&website=5b29c824a85d0a7df5c40080;5b29c821a85d0a7df5c3ff22&start_date=2018-05-01&end_date=2018-05-02
 * 
 * SELECT brand, website_name, SUM(reputation) AS count FROM
 * ibuzz_voc.brand_reputation where brand in ('BENZ', 'BMW') and website_id
 * in('5b29c824a85d0a7df5c40080', '5b29c821a85d0a7df5c3ff22') and
 * DATE_FORMAT(date, '%Y-%m-%d') >= '2018-05-01' AND DATE_FORMAT(date,
 * '%Y-%m-%d') <= '2018-05-02' GROUP BY brand, website_id;
 * http://localhost:8080/voc/industry/total-count.jsp?brand=BENZ;BMW&website=5b29c824a85d0a7df5c40080;5b29c821a85d0a7df5c3ff22&start_date=2018-05-01&end_date=2018-05-02
 * http://localhost:8080/voc/industry/total-count.jsp?website=5b29c824a85d0a7df5c40080;5b29c821a85d0a7df5c3ff22&brand=BENZ;BMW&start_date=2018-05-01&end_date=2018-05-02
 * 
 * Requirement Change: on 2018/12/10(一): 1. 新增 limit 參數: Default: 10 2. 修改
 * website 參數，由原本吃 website ID 改為吃 website name
 * 
 * EX: SELECT brand, website_name, SUM(reputation) AS count FROM
 * ibuzz_voc.brand_reputation where brand in ('BENZ', 'BMW') and website_name
 * in('PTT', 'Mobile01') and DATE_FORMAT(date, '%Y-%m-%d') >= '2018-05-01' AND
 * DATE_FORMAT(date, '%Y-%m-%d') <= '2018-05-02' GROUP BY brand, website_id
 * ORDER BY count DESC LIMIT 5;
 * http://localhost:8080/voc/industry/total-count.jsp?brand=BENZ;BMW&website=PTT;Mobile01&start_date=2018-05-01&end_date=2018-05-02&limit=5
 * http://localhost:8080/voc/industry/total-count.jsp?website=PTT;Mobile01&brand=BENZ;BMW&start_date=2018-05-01&end_date=2018-05-02
 * 
 * Requirement Change: 1.加上 sentiment(評價): 1:偏正、0:中性、-1:偏負 EX: SELECT brand,
 * sentiment, SUM(reputation) AS count FROM ibuzz_voc.brand_reputation where
 * brand in ('MAZDA','BENZ') and sentiment in('1','0','-1') and
 * DATE_FORMAT(date, '%Y-%m-%d') >= '2019-01-01' AND DATE_FORMAT(date,
 * '%Y-%m-%d') <= '2019-03-31' GROUP BY brand, sentiment ORDER BY count DESC
 * LIMIT 10;
 * http://localhost:8080/voc/industry/total-count.jsp?brand=MAZDA;BENZ&sentiment=1;0;-1&start_date=2019-01-01&end_date=2019-03-31&limit=10
 * 
 * Requirement Change: 加參數:monitor_brand(監測品牌範圍): Ex:
 * http://localhost:8080/voc/industry/total-count.jsp?brand=MAZDA;BENZ&sentiment=1;0;-1&start_date=2019-01-01&end_date=2019-03-31&limit=10
 * http://localhost:8080/voc/industry/total-count.jsp?brand=MAZDA;BENZ&sentiment=1;0;-1&monitor_brand=MAZDA&start_date=2019-01-01&end_date=2019-03-31&limit=10
 * 
 * Note: 呼叫API時所下的參數若包含 product 或 series, 就使用 ibuzz_voc.product_reputation
 * (產品表格), 否則就使用 ibuzz_voc.brand_reputation (品牌表格) ==>See RootAPI.java
 * 
 * Requirement Change: 1.參數:source修改為media_type(來源類型): 2.項目名稱 channel
 * 顯示由channel_name改為channel_display_name 3.前端改用POST method.
 * 
 * Requirement Change: 1.新增output值(title_count, content_count, comment_count)
 * Ex:
 * [{"key":"brand","value":"BENZ;LEXUS;TOYOTA;PORSCHE","description":""},{"key":"media_type","value":"sns;forum","description":""},{"key":"start_date","value":"2019-05-01","description":""},{"key":"end_date","value":"2019-05-05","description":""}]
 * 
 * Requirement Change: 1.新增參數:sorting (reputation、title、content、comment)
 * 
 * Requirement Change: 1.新增參數:features (只要有帶features參數就只查詢feature_reputation)
 */
public class TotalCount extends RootAPI {
	private static final Logger LOGGER = LoggerFactory.getLogger(TotalCount.class);
	private Map<String, String[]> orderedParameterMap = new ParameterMap<>();
	private String selectTotalCountSQL;
	private String selectUpdateTimeSQL;
	private List<String> itemNameList = new ArrayList<>();
	private Map<String, Integer> hash_itemName_count = new HashMap<>();
	// private String tableName;
	private int limit = 10; // Default: 10
	private String sorting = "count"; // Default: reputation
	private JSONArray sortedJsonArray;

	@Override
	public String processRequest(HttpServletRequest request) {
		JSONObject errorResponse = this.validateAndSetOrderedParameterMap(request);
		if (errorResponse != null) {
			return errorResponse.toString();
		}

		JSONArray itemArray = new JSONArray();
		int total_count = 0;
		String update_time = "";

		if (hasFeatures(orderedParameterMap)) {
			JSONObject featureJobj = new JSONObject();
			JSONObject commentJobj = new JSONObject();
			JSONObject jobj = new JSONObject();
			featureJobj = this.queryForFeatures(TABLE_FEATURE_REPUTATION);
			commentJobj = this.queryForFeatures(TABLE_FEATURE_REPUTATION_COMMENT);
			if (featureJobj.length() <= 0 && commentJobj.length() <= 0) {
				jobj = ApiResponse.successTemplate();
				jobj.put("result", "{}");
				return jobj.toString();
			}

			boolean querySuccess = mergeJSONObjects(featureJobj, commentJobj, itemArray);
			if (querySuccess && itemArray != null) {
				if (itemArray.length() > 0) {
					total_count = this.getTotalCount(itemArray);
					update_time = this.queryUpdateTime(this.selectUpdateTimeSQL);
				}

				int desc_remainingCnt = this.limit - itemArray.length();
				if (desc_remainingCnt > 0) {
					for (String itemName : itemNameList) {
						Integer count = hash_itemName_count.get(itemName);

						if (desc_remainingCnt > 0) {
							if (count == null) {
								JSONObject itemObject = new JSONObject();
								itemObject.put("item", itemName);
								itemObject.put("count", 0);
								itemArray.put(itemObject);
								// LOGGER.info("itemObject to be add: " + itemObject);
								desc_remainingCnt--;
							}
						}
					}
				}
				this.sortedJsonArray = this.getSortedResultArray(itemArray, this.limit);
				JSONObject successObject = ApiResponse.successTemplate();
				successObject.put("total_count", total_count);
				successObject.put("update_time", update_time);
				successObject.put("result", this.sortedJsonArray);
				return successObject.toString();
			}
		} //

		if (hasSentiment(orderedParameterMap)) {
			JSONObject brandOrProductJobj = new JSONObject();
			JSONObject commentJobj = new JSONObject();
			JSONObject jobj = new JSONObject();
			String table =  getTableName(orderedParameterMap);
			String table_comment = table + "_comment";
			brandOrProductJobj = this.queryForSentiment(table);
			commentJobj = this.queryForSentiment(table_comment);

			if (brandOrProductJobj.length() <= 0 && commentJobj.length() <= 0) {
				jobj = ApiResponse.successTemplate();
				jobj.put("result", "{}");
				return jobj.toString();
			}
			
			boolean querySuccess = mergeJSONObjects(brandOrProductJobj, commentJobj, itemArray);
			if (querySuccess && itemArray != null) {
				if (itemArray.length() > 0) {
					total_count = this.getTotalCount(itemArray);
					update_time = this.queryUpdateTime(this.selectUpdateTimeSQL);
				}

				int desc_remainingCnt = this.limit - itemArray.length();
				if (desc_remainingCnt > 0) {
					for (String itemName : itemNameList) {
						Integer count = hash_itemName_count.get(itemName);

						if (desc_remainingCnt > 0) {
							if (count == null) {
								JSONObject itemObject = new JSONObject();
								itemObject.put("item", itemName);
								itemObject.put("count", 0);
								itemArray.put(itemObject);
								// LOGGER.info("itemObject to be add: " + itemObject);
								desc_remainingCnt--;
							}
						}
					}
				}
				this.sortedJsonArray = this.getSortedResultArray(itemArray, this.limit);
				JSONObject successObject = ApiResponse.successTemplate();
				successObject.put("total_count", total_count);
				successObject.put("update_time", update_time);
				successObject.put("result", this.sortedJsonArray);
				return successObject.toString();
			}
		} //

		String tableName = getTableName(orderedParameterMap);
		itemArray = this.queryData(tableName);
		if (itemArray != null) {
			if (itemArray.length() > 0) {
				total_count = this.queryToltalCount(this.selectTotalCountSQL);
				update_time = this.queryUpdateTime(this.selectUpdateTimeSQL);
			}
			JSONObject successObject = ApiResponse.successTemplate();
			successObject.put("total_count", total_count);
			successObject.put("update_time", update_time);
			successObject.put("result", itemArray);
//			LOGGER.info("responseJsonStr=" + successObject.toString());
			return successObject.toString();
		}

		return ApiResponse.unknownError().toString();
	}

	private boolean hasFeatures(Map<String, String[]> paramMap) {
		return paramMap.containsKey("features");
	}

	private boolean hasSentiment(Map<String, String[]> paramMap) {
		return paramMap.containsKey("sentiment");
	}

	private JSONObject queryForFeatures(String table_name) {
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		StringBuffer selectSQL = new StringBuffer();
		try {
			selectSQL.append(this.genSelectClause(table_name));
			selectSQL.append(this.genWhereClause());
			selectSQL.append(this.genGroupByOrderByClause());
			selectSQL.append("LIMIT ? ");

			conn = DBUtil.getConn();
			preparedStatement = conn.prepareStatement(selectSQL.toString());
			this.setWhereClauseValues(preparedStatement);

			String psSQLStr = preparedStatement.toString();
			LOGGER.debug("psSQLStr = " + psSQLStr);
			this.selectUpdateTimeSQL = "SELECT MAX(DATE_FORMAT(update_time, '%Y-%m-%d %H:%i:%s')) AS " + UPDATE_TIME
					+ psSQLStr.substring(psSQLStr.indexOf(" FROM "), psSQLStr.indexOf(" GROUP BY "));
			// LOGGER.debug("selectUpdateTimeSQL = " + this.selectUpdateTimeSQL);

			JSONObject jobj = new JSONObject();
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				StringBuffer item = new StringBuffer();
				int i = 0;
				for (Map.Entry<String, String[]> entry : this.orderedParameterMap.entrySet()) {
					String paramName = entry.getKey();
					if ("monitor_brand".equals(paramName)) {
						continue;
					}
					if ("industry".equals(paramName)) {
						continue;
					}
					String columnName = this.getColumnName(paramName);
					if ("channel_id".equals(columnName)) {
						columnName = "channel_display_name";
					}
					if (!"rep_date".equals(columnName)) {
						String s = rs.getString(columnName);
						if ("sentiment".equals(paramName)) {
							s = EnumSentiment.getEnum(s).getName();
						}
						if (i == 0) {
							item.append(s);
						} else {
							item.append("-").append(s);
						}
					}
					i++;
				}
				int count = rs.getInt("count");
				jobj.put(item.toString(), count);
			}
			LOGGER.info("Out jobj: " + jobj);
			return jobj;

		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, preparedStatement, conn);
		}
		return null;
	}

	private JSONObject queryForSentiment(String table_name) {
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		StringBuffer selectSQL = new StringBuffer();
		try {
			selectSQL.append(this.genSelectClause(table_name));
			selectSQL.append(this.genWhereClause());
			selectSQL.append(this.genGroupByOrderByClause());
			selectSQL.append("LIMIT ? ");

			conn = DBUtil.getConn();
			preparedStatement = conn.prepareStatement(selectSQL.toString());
			this.setWhereClauseValues(preparedStatement);

			String psSQLStr = preparedStatement.toString();
			LOGGER.debug("psSQLStr = " + psSQLStr);
			this.selectUpdateTimeSQL = "SELECT MAX(DATE_FORMAT(update_time, '%Y-%m-%d %H:%i:%s')) AS " + UPDATE_TIME
					+ psSQLStr.substring(psSQLStr.indexOf(" FROM "), psSQLStr.indexOf(" GROUP BY "));
			// LOGGER.debug("selectUpdateTimeSQL = " + this.selectUpdateTimeSQL);

			JSONObject jobj = new JSONObject();
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				StringBuffer item = new StringBuffer();
				int i = 0;
				for (Map.Entry<String, String[]> entry : this.orderedParameterMap.entrySet()) {
					String paramName = entry.getKey();
					if ("monitor_brand".equals(paramName)) {
						continue;
					}
					if ("industry".equals(paramName)) {
						continue;
					}
					String columnName = this.getColumnName(paramName);
					if ("channel_id".equals(columnName)) {
						columnName = "channel_display_name";
					}
					if (!"rep_date".equals(columnName)) {
						String s = rs.getString(columnName);
						if ("sentiment".equals(paramName)) {
							s = EnumSentiment.getEnum(s).getName();
						}
						if (i == 0) {
							item.append(s);
						} else {
							item.append("-").append(s);
						}
					}
					i++;
				}
				int count = rs.getInt("count");
				jobj.put(item.toString(), count);
			}
			LOGGER.info("Out jobj: " + jobj);
			return jobj;

		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, preparedStatement, conn);
		}
		return null;
	}

	private JSONArray queryData(String table_name) {
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		StringBuffer selectSQL = new StringBuffer();
		try {
			selectSQL.append(this.genSelectClause(table_name));
			selectSQL.append(this.genWhereClause());
			selectSQL.append(this.genGroupByOrderByClause());
			selectSQL.append("LIMIT ? ");
			// LOGGER.debug("selectSQL=" + selectSQL.toString());

			conn = DBUtil.getConn();
			preparedStatement = conn.prepareStatement(selectSQL.toString());
			this.setWhereClauseValues(preparedStatement);

			String psSQLStr = preparedStatement.toString();
			LOGGER.debug("psSQLStr = " + psSQLStr);
			this.selectUpdateTimeSQL = "SELECT MAX(DATE_FORMAT(update_time, '%Y-%m-%d %H:%i:%s')) AS " + UPDATE_TIME
					+ psSQLStr.substring(psSQLStr.indexOf(" FROM "), psSQLStr.indexOf(" GROUP BY "));
			// LOGGER.debug("selectUpdateTimeSQL = " + this.selectUpdateTimeSQL);
			this.selectTotalCountSQL = "SELECT SUM(reputation) AS " + TOTAL_COUNT
					+ psSQLStr.substring(psSQLStr.indexOf(" FROM "), psSQLStr.indexOf(" GROUP BY "));
			LOGGER.debug("selectTotalCountSQL = " + this.selectTotalCountSQL);

			JSONArray itemArray = new JSONArray();
			Map<String, Integer> hash_itemName_count = new HashMap<>();
			Map<String, Integer> hash_itemName_title = new HashMap<>();
			Map<String, Integer> hash_itemName_content = new HashMap<>();
			Map<String, Integer> hash_itemName_comment = new HashMap<>();
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				StringBuffer item = new StringBuffer();
				int i = 0;
				for (Map.Entry<String, String[]> entry : this.orderedParameterMap.entrySet()) {
					String paramName = entry.getKey();
					if ("monitor_brand".equals(paramName)) {
						continue;
					}
					if ("industry".equals(paramName)) {
						continue;
					}
					String columnName = this.getColumnName(paramName);
					if ("channel_id".equals(columnName)) {
						columnName = "channel_display_name";
					}
					if (!"rep_date".equals(columnName)) {
						String s = rs.getString(columnName);
						if ("sentiment".equals(paramName)) {
							s = EnumSentiment.getEnum(s).getName();
						}
						if (i == 0) {
							item.append(s);
						} else {
							item.append("-").append(s);
						}
					}
					i++;
				}
				int count = rs.getInt("count");
				int title_count = rs.getInt("title_count");
				int content_count = rs.getInt("content_count");
				int comment_count = rs.getInt("comment_count");
				// LOGGER.debug("item=" + item.toString() + ", count=" + count);

				hash_itemName_count.put(item.toString(), count);
				hash_itemName_title.put(item.toString(), title_count);
				hash_itemName_content.put(item.toString(), content_count);
				hash_itemName_comment.put(item.toString(), comment_count);

				JSONObject itemObject = new JSONObject();
				itemObject.put("item", item.toString());
				itemObject.put("count", count);
				itemObject.put("title_count", title_count);
				itemObject.put("content_count", content_count);
				itemObject.put("comment_count", comment_count);
				itemArray.put(itemObject);
			}

			int desc_remainingCnt = this.limit - itemArray.length();
			if (desc_remainingCnt > 0) {
				for (String itemName : itemNameList) {
					Integer count = hash_itemName_count.get(itemName);
					Integer title_count = hash_itemName_title.get(itemName);
					Integer content_count = hash_itemName_content.get(itemName);
					Integer comment_count = hash_itemName_comment.get(itemName);
					// LOGGER.debug("itemName=" + itemName);

					if (desc_remainingCnt > 0) {
						if (count == null && title_count == null && content_count == null && comment_count == null) {
							JSONObject itemObject = new JSONObject();
							itemObject.put("item", itemName);
							itemObject.put("count", 0);
							itemObject.put("title_count", 0);
							itemObject.put("content_count", 0);
							itemObject.put("comment_count", 0);
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

		String[] trimedValues = new String[] { trimedValuesSB.toString() };
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
		Map<String, String[]> parameterMap = request.getParameterMap();
		JSONObject errorResponse = this.checkDateParameters(parameterMap);
		if (errorResponse != null) {
			return errorResponse;
		}
		// this.tableName = this.getTableName(parameterMap);

		String[] paramValues_industry = null;
		String[] paramValues_brand = null;
		String[] paramValues_series = null;
		String[] paramValues_product = null;
		String[] paramValues_mediaType = null;
		String[] paramValues_website = null;
		String[] paramValues_channel = null;
		String[] paramValues_sentiment = null;
		String[] paramValues_features = null;
		String[] paramValues_monitorBrand = null;
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

		String sortingStr = StringUtils.trimToEmpty(request.getParameter("sorting"));
		if (!StringUtils.isEmpty(sortingStr)) {
			switch (sortingStr) {
			case "reputation":
				this.sorting = "count";
				break;
			case "title":
				this.sorting = "title_count";
				break;
			case "content":
				this.sorting = "content_count";
				break;
			case "comment":
				this.sorting = "comment_count";
				break;
			}
		}

		for (Map.Entry<String, String[]> entry : parameterMap.entrySet()) {
			String paramName = entry.getKey();
			if (API_KEY.equals(paramName))
				continue;

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
			case PARAM_COLUMN_INDUSTRY:
				paramValues_industry = trimedValues;
				break;
			case PARAM_COLUMN_BRAND:
				paramValues_brand = trimedValues;
				break;
			case PARAM_COLUMN_SERIES:
				paramValues_series = trimedValues;
				break;
			case PARAM_COLUMN_PRODUCT:
				paramValues_product = trimedValues;
				break;
			case PARAM_COLUMN_MEDIA_TYPE:
				paramValues_mediaType = trimedValues;
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
			case PARAM_COLUMN_FEATURES:
				paramValues_features = trimedValues;
				break;
			case PARAM_COLUMN_MONITORBRAND:
				paramValues_monitorBrand = trimedValues;
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
		String[] trdItemArr = null;
		int itemCnt = 0;
		if (paramValues_industry != null) {
			String paramName = EnumTotalCount.PARAM_COLUMN_INDUSTRY.getParamName();
			this.orderedParameterMap.put(paramName, paramValues_industry);
			/**
			 * if (itemCnt == 0) { mainItemArr =
			 * paramValues_industry[0].split(PARAM_VALUES_SEPARATOR); } else if (itemCnt ==
			 * 1) { secItemArr = paramValues_industry[0].split(PARAM_VALUES_SEPARATOR); }
			 * itemCnt++;
			 **/
		}
		if (paramValues_brand != null) {
			String paramName = EnumTotalCount.PARAM_COLUMN_BRAND.getParamName();
			this.orderedParameterMap.put(paramName, paramValues_brand);
			if (itemCnt == 0) {
				mainItemArr = paramValues_brand[0].split(PARAM_VALUES_SEPARATOR);
			} else if (itemCnt == 1) {
				secItemArr = paramValues_brand[0].split(PARAM_VALUES_SEPARATOR);
			} else if (itemCnt == 2) {
				trdItemArr = paramValues_brand[0].split(PARAM_VALUES_SEPARATOR);
			}
			itemCnt++;
		}
		if (paramValues_series != null) {
			String paramName = EnumTotalCount.PARAM_COLUMN_SERIES.getParamName();
			this.orderedParameterMap.put(paramName, paramValues_series);
			if (itemCnt == 0) {
				mainItemArr = paramValues_series[0].split(PARAM_VALUES_SEPARATOR);
			} else if (itemCnt == 1) {
				secItemArr = paramValues_series[0].split(PARAM_VALUES_SEPARATOR);
			} else if (itemCnt == 2) {
				trdItemArr = paramValues_series[0].split(PARAM_VALUES_SEPARATOR);
			}
			itemCnt++;
		}
		if (paramValues_product != null) {
			String paramName = EnumTotalCount.PARAM_COLUMN_PRODUCT.getParamName();
			this.orderedParameterMap.put(paramName, paramValues_product);
			if (itemCnt == 0) {
				mainItemArr = paramValues_product[0].split(PARAM_VALUES_SEPARATOR);
			} else if (itemCnt == 1) {
				secItemArr = paramValues_product[0].split(PARAM_VALUES_SEPARATOR);
			} else if (itemCnt == 2) {
				trdItemArr = paramValues_product[0].split(PARAM_VALUES_SEPARATOR);
			}
			itemCnt++;
		}
		if (paramValues_mediaType != null) {
			String paramName = EnumTotalCount.PARAM_COLUMN_MEDIA_TYPE.getParamName();
			this.orderedParameterMap.put(paramName, paramValues_mediaType);
			if (itemCnt == 0) {
				mainItemArr = paramValues_mediaType[0].split(PARAM_VALUES_SEPARATOR);
			} else if (itemCnt == 1) {
				secItemArr = paramValues_mediaType[0].split(PARAM_VALUES_SEPARATOR);
			} else if (itemCnt == 2) {
				trdItemArr = paramValues_mediaType[0].split(PARAM_VALUES_SEPARATOR);
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
			} else if (itemCnt == 2) {
				trdItemArr = paramValues_website[0].split(PARAM_VALUES_SEPARATOR);
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
			} else if (itemCnt == 2) {
				trdItemArr = paramValues_channel[0].split(PARAM_VALUES_SEPARATOR);
				for (int i = 0; i < trdItemArr.length; i++) {
					String trdValue = trdItemArr[i];
					// secItemArr[i] = this.getChannelNameById(this.tableName, secValue);
					trdItemArr[i] = this.getChannelNameById(trdValue);
				}
			}
			itemCnt++;
		}
		if (paramValues_sentiment != null) {
			String paramName = EnumTotalCount.PARAM_COLUMN_SENTIMENT.getParamName();
			this.orderedParameterMap.put(paramName, paramValues_sentiment);
			if (itemCnt == 0) {
				mainItemArr = paramValues_sentiment[0].split(PARAM_VALUES_SEPARATOR);
				for (int i = 0; i < mainItemArr.length; i++) {
					String mainValue = mainItemArr[i];
					mainItemArr[i] = EnumSentiment.getEnum(mainValue).getName();
				}
			} else if (itemCnt == 1) {
				secItemArr = paramValues_sentiment[0].split(PARAM_VALUES_SEPARATOR);
				for (int i = 0; i < secItemArr.length; i++) {
					String secValue = secItemArr[i];
					secItemArr[i] = EnumSentiment.getEnum(secValue).getName();
				}
			} else if (itemCnt == 2) {
				trdItemArr = paramValues_sentiment[0].split(PARAM_VALUES_SEPARATOR);
				for (int i = 0; i < trdItemArr.length; i++) {
					String trdValue = trdItemArr[i];
					trdItemArr[i] = EnumSentiment.getEnum(trdValue).getName();
				}
			}
			itemCnt++;
		}
		if (paramValues_features != null) {
			String paramName = EnumTotalCount.PARAM_COLUMN_FEATURES.getParamName();
			this.orderedParameterMap.put(paramName, paramValues_features);
			if (itemCnt == 0) {
				mainItemArr = paramValues_features[0].split(PARAM_VALUES_SEPARATOR);
			} else if (itemCnt == 1) {
				secItemArr = paramValues_features[0].split(PARAM_VALUES_SEPARATOR);
			} else if (itemCnt == 2) {
				trdItemArr = paramValues_features[0].split(PARAM_VALUES_SEPARATOR);
			}
			itemCnt++;
		}
		if (paramValues_monitorBrand != null) {
			String paramName = EnumTotalCount.PARAM_COLUMN_MONITORBRAND.getParamName();
			this.orderedParameterMap.put(paramName, paramValues_monitorBrand);
		}
		if (itemCnt == 0) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}

		if (mainItemArr != null) {
			for (String mainItem : mainItemArr) {
				if (secItemArr != null) {
					for (String secItem : secItemArr) {
						if (trdItemArr != null) {
							for (String trdItem : trdItemArr) {
								itemNameList.add(mainItem + "-" + secItem + "-" + trdItem);
							}
						} else {
							itemNameList.add(mainItem + "-" + secItem);
						}
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

	private String genSelectClause(String table_name) {
		StringBuffer selectClauseSB = new StringBuffer();
		selectClauseSB.append("SELECT ");
		int i = 0;
		for (Map.Entry<String, String[]> entry : this.orderedParameterMap.entrySet()) {
			String paramName = entry.getKey();
			if (this.isItemParamName(paramName)) {
				String columnName = this.getColumnName(paramName);
				if ("website_id".equals(columnName)) {
					columnName = "website_name";
				} else if ("channel_id".equals(columnName)) {
					columnName = "channel_display_name";
				}
				if (i == 0) {
					selectClauseSB.append(columnName);
				} else {
					selectClauseSB.append(" ,").append(columnName);
				}
				i++;
			}
		}
		if (table_name.equals(TABLE_BRAND_REPUTATION) || table_name.equals(TABLE_PRODUCT_REPUTATION)) {
			if (this.hasSentiment(orderedParameterMap)) {
				selectClauseSB.append(" ,").append("count(DISTINCT id) AS count FROM ").append(table_name).append(" ");
			} else {
				selectClauseSB.append(" ,").append(
						"SUM(reputation) AS count, SUM(title_hit) AS title_count, SUM(content_hit) AS content_count, SUM(comment_hit) AS comment_count FROM ")
						.append(table_name).append(" ");
			}
		} else {
			selectClauseSB.append(" ,").append("count(DISTINCT id) AS count FROM ").append(table_name).append(" ");
		}
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
				whereClauseSB.append("DATE_FORMAT(rep_date, '%Y-%m-%d') >= ? ");
			} else if (paramName.equals("end_date")) {
				whereClauseSB.append("DATE_FORMAT(rep_date, '%Y-%m-%d') <= ? ");
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
			if ("monitor_brand".equals(paramName)) {
				continue;
			}
			String columnName = this.getColumnName(paramName);
			if (!"rep_date".equals(columnName)) {
				if (i == 0) {
					columnsSB.append(columnName);
				} else {
					columnsSB.append(", ").append(columnName);
				}
			}
			i++;
		}
		groupByOrderByClauseSB.append("GROUP BY ").append(columnsSB.toString()).append(" ");
		groupByOrderByClauseSB.append("ORDER BY ").append(this.sorting).append(" ").append("DESC ");
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

	private boolean mergeJSONObjects(JSONObject json1, JSONObject json2, JSONArray resArray) {
		JSONObject mergedJSON = new JSONObject();
		try {
			if (json1.length() > 0) {
				mergedJSON = new JSONObject(json1, JSONObject.getNames(json1));
			}
			if (json2.length() > 0) {
				for (String featureKey : JSONObject.getNames(json2)) {
					if (mergedJSON.has(featureKey)) {
						mergedJSON.put(featureKey, (Integer) json2.get(featureKey) + (Integer) json1.get(featureKey));
					} else {
						mergedJSON.put(featureKey, json2.get(featureKey));
					}
				}
			}
			LOGGER.info("mergedJSON: " + mergedJSON);

			Iterator<String> iterKeys = mergedJSON.keys();
			while (iterKeys.hasNext()) {
				String key = iterKeys.next();
				JSONObject resJobj = new JSONObject();

				resJobj.put("item", key);
				resJobj.put("count", mergedJSON.get(key));
				this.hash_itemName_count.put(key, (Integer) mergedJSON.get(key));
				resArray.put(resJobj);
			}
			// this.sortedJsonArray = this.getSortedResultArray(resArray, nLimit);
			return true;
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		}
		return false;
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
					valA = (Integer) a.get("count");
					valB = (Integer) b.get("count");
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
			sortedJsonArray.put(jsonObject);
		}

		return sortedJsonArray;
	}

	// for sorting
	private Integer getTotalCount(JSONArray dataArray) {
		Integer totalCount = 0;
		for (int i = 0; i < dataArray.length(); i++) {
			JSONObject dataObject = dataArray.getJSONObject(i);
			totalCount += dataObject.getInt(this.sorting);
		}
		return totalCount;
	}
}
