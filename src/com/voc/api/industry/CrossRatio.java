package com.voc.api.industry;

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

/**
 * 查詢產業分析交叉分析口碑總數:
 * - 查詢時間區間內各項目交叉分析的口碑總數及比例
 * - /industry/cross-ratio.jsp
 * 
 * Params:
 * query	*main_filter	string	主要分析項目
 * query	*main_value		string	主要欲分析項目值
 * query	*sec_filter		string	次要分析項目
 * query	*sec_value		string	次要欲分析項目值
 * query	*start_date		string	欲查詢起始日期
 * query	*end_date		string	欲查詢結束日期
 * query	 limit			string	顯示項目筆數	Default: 10 ==>注意: 這邊的limit筆數,是指 Main Item 的筆數。不是直接在SQL中,下limit喔!
 * query	*api_key		string	MORE API token
 * 
 * SELECT brand, website_name, SUM(reputation) AS count FROM ibuzz_voc.brand_reputation WHERE brand IN ('BENZ', 'BMW') AND website_id IN('5b29c824a85d0a7df5c40080', '5b29c821a85d0a7df5c3ff22') AND DATE_FORMAT(date, '%Y-%m-%d') >= '2018-05-01' AND DATE_FORMAT(date, '%Y-%m-%d') <= '2018-05-02' GROUP BY brand, website_id;
 * http://localhost:8080/voc/industry/cross-ratio.jsp?main_filter=brand&main_value=BENZ;BMW&sec_filter=website&sec_value=5b29c824a85d0a7df5c40080;5b29c821a85d0a7df5c3ff22&start_date=2018-05-01&end_date=2018-05-02
 * 
 * Requirement Change: on 2018/12/10(一):
 * 1. 新增 limit 參數:  Default: 10
 * 2. 修改 website 參數，由原本吃 website ID 改為吃 website name
 * 
 * EX:
 * SELECT brand, website_name, SUM(reputation) AS count FROM ibuzz_voc.brand_reputation WHERE brand IN ('BENZ', 'BMW') AND website_name IN('PTT', 'Mobile01') AND DATE_FORMAT(date, '%Y-%m-%d') >= '2018-05-01' AND DATE_FORMAT(date, '%Y-%m-%d') <= '2018-05-02' GROUP BY brand, website_id ORDER BY count DESC;
 * http://localhost:8080/voc/industry/cross-ratio.jsp?main_filter=brand&main_value=BENZ;BMW&sec_filter=website&sec_value=PTT;Mobile01&start_date=2018-05-01&end_date=2018-05-02&limit=5
 * 
 * Requirement Change: 
 * 1.加上 sentiment(評價): 1:偏正、0:中性、-1:偏負
 * EX:
 * http://localhost:8080/voc/industry/cross-ratio.jsp?main_filter=brand&main_value=MAZDA;BENZ&sec_filter=sentiment&sec_value=1;0;-1&start_date=2019-01-01&end_date=2019-03-31&limit=10
 * 
 * Requirement Change: 加參數:monitor_brand(監測品牌範圍):
 * Ex: 
 * http://localhost:8080/voc/industry/cross-ratio.jsp?main_filter=brand&main_value=MAZDA;BENZ;TOYOTA&sec_filter=sentiment&sec_value=1;0;-1&start_date=2019-01-01&end_date=2019-03-31&limit=10
 * http://localhost:8080/voc/industry/cross-ratio.jsp?main_filter=brand&main_value=MAZDA;BENZ;TOYOTA&sec_filter=sentiment&sec_value=1;0;-1&monitor_brand=MAZDA;BENZ&start_date=2019-01-01&end_date=2019-03-31&limit=10
 * 
 * Requirement Change: 
 * 1.參數:source修改為media_type(來源類型):
 * 2.項目名稱 channel 顯示由channel_name改為channel_display_name
 * 3.前端改用POST method.
 * 
 * Requirement Change: 
 * 新增output值(title_count, content_count, comment_count)
 * Ex:
 * [{"key":"main_filter","value":"brand","description":""},{"key":"main_value","value":"BENZ;TOYOTA;PORSCHE","description":""},{"key":"sec_filter","value":"media_type","description":""},{"key":"sec_value","value":"forum;sns","description":""},{"key":"start_date","value":"2019-05-01","description":""},{"key":"end_date","value":"2019-05-05","description":""}]

 */
public class CrossRatio extends RootAPI {
	private static final Logger LOGGER = LoggerFactory.getLogger(CrossRatio.class);
	private String mainFilter = null;
	private String mainValue = null;
	private String secFilter = null;
	private String secValue = null;
	private String monitor_brand = null;
	private String startDate = null;
	private String endDate = null;
	private int limit = 10; // Default: 10
	
	private String[] mainValueArr = null;
	private String[] secValueArr = null;
	private String[] monitorBrandArr = null;

	private String mainSelectCol = null;
	private String secSelectCol = null;
	
	private String selectUpdateTimeSQL;
	
	private String tableName;
	
	@Override
	public String processRequest(HttpServletRequest request) {
		this.requestAndTrimParams(request);
		JSONObject errorResponse = this.validateParams();
		if (errorResponse != null) {
			return errorResponse.toString();
		}
		JSONArray resultArray = this.queryData();
		String update_time = "";
		if (resultArray != null) {
			if (resultArray.length() > 0) {
				update_time = this.queryUpdateTime(this.selectUpdateTimeSQL);
			}
			JSONObject successObject = ApiResponse.successTemplate();
			successObject.put("update_time", update_time);
			successObject.put("result", resultArray);
			// LOGGER.info("responseJsonStr=" + successObject.toString());
			return successObject.toString();
		}
		return ApiResponse.unknownError().toString();
	}
	
	private void requestAndTrimParams(HttpServletRequest request) {
		this.mainFilter = StringUtils.trimToEmpty(request.getParameter("main_filter"));
		this.mainValue = StringUtils.trimToEmpty(request.getParameter("main_value"));
		this.secFilter = StringUtils.trimToEmpty(request.getParameter("sec_filter"));
		this.secValue = StringUtils.trimToEmpty(request.getParameter("sec_value"));
		this.monitor_brand = StringUtils.trimToEmpty(request.getParameter("monitor_brand"));
		this.startDate = StringUtils.trimToEmpty(request.getParameter("start_date"));
		this.endDate = StringUtils.trimToEmpty(request.getParameter("end_date"));
		
		String limitStr = StringUtils.trimToEmpty(request.getParameter("limit"));
		if (!StringUtils.isEmpty(limitStr)) {
			try {
				this.limit = Integer.parseInt(limitStr);
			} catch (Exception e) {
				LOGGER.error(e.getMessage());
			}
		}
		
		this.mainValue = this.trimValues(this.mainValue);
		this.secValue = this.trimValues(this.secValue);
	}
	
	/**
	 * Trim values separated by PARAM_VALUES_SEPARATOR.
	 * 
	 */
	private String trimValues(String values) {
		StringBuffer trimedValuesSB = new StringBuffer();
		String[] valueArr = values.split(PARAM_VALUES_SEPARATOR);
		int i = 0;
		for (String value : valueArr) {
			String trimedValue = StringUtils.trimToEmpty(value);
			if (i == 0) {
				trimedValuesSB.append(trimedValue);
			} else {
				trimedValuesSB.append(PARAM_VALUES_SEPARATOR).append(trimedValue);
			}
			i++;
		}
		return trimedValuesSB.toString();
	}
	
	private JSONObject validateParams() {
		if (StringUtils.isBlank(this.mainFilter) || StringUtils.isBlank(this.mainValue)
				|| StringUtils.isBlank(this.secFilter) || StringUtils.isBlank(this.secValue)
				|| StringUtils.isBlank(this.startDate) || StringUtils.isBlank(this.endDate)) {

			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}
		
		if (!Common.isValidDate(this.startDate, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid start_date.");
		}
		this.startDate = Common.formatDate(this.startDate, "yyyy-MM-dd");
		
		if (!Common.isValidDate(this.endDate, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid end_date.");
		}
		this.endDate = Common.formatDate(this.endDate, "yyyy-MM-dd");
		
		if (!Common.isValidStartDate(this.startDate, this.endDate, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid period values.");
		}
		return null;
	}
	
	private JSONArray queryData() {
		String mainFilterColumn = this.getColumnName(mainFilter);
		String secFilterColumn = this.getColumnName(secFilter);
		
		List<String> paramNameList = new ArrayList<>();
		paramNameList.add(mainFilter);
		paramNameList.add(secFilter);
		this.tableName = this.getTableName(paramNameList);
		
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		StringBuffer selectSQL = new StringBuffer();
		try {
			mainValueArr = mainValue.split(PARAM_VALUES_SEPARATOR);
			secValueArr = secValue.split(PARAM_VALUES_SEPARATOR);
			monitorBrandArr = monitor_brand.split(PARAM_VALUES_SEPARATOR);
			selectSQL.append(this.genSelectSQL(tableName, mainFilterColumn, secFilterColumn, mainValueArr, secValueArr, monitorBrandArr));
			// LOGGER.debug("selectSQL: " + selectSQL.toString());
			
			conn = DBUtil.getConn();
			preparedStatement = conn.prepareStatement(selectSQL.toString());
			this.setWhereClauseValues(preparedStatement, mainValueArr, secValueArr, monitorBrandArr, startDate, endDate);
			
			String psSQLStr = preparedStatement.toString();
			LOGGER.debug("psSQLStr = " + psSQLStr);
			this.selectUpdateTimeSQL = "SELECT MAX(DATE_FORMAT(update_time, '%Y-%m-%d %H:%i:%s')) AS " + UPDATE_TIME + psSQLStr.substring(psSQLStr.indexOf(" FROM "), psSQLStr.indexOf(" GROUP BY "));
			LOGGER.debug("selectUpdateTimeSQL = " + this.selectUpdateTimeSQL);
			
			Map<String, Map<String, Integer>> hash_mainItem_secItem = new HashMap<>();
			Map<String, Map<String, Integer>> hash_mainItem_secItem_title = new HashMap<>();
			Map<String, Map<String, Integer>> hash_mainItem_secItem_content = new HashMap<>();
			Map<String, Map<String, Integer>> hash_mainItem_secItem_comment = new HashMap<>();
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				String main_item = rs.getString(this.mainSelectCol);
				String sec_item = rs.getString(this.secSelectCol);
				if ("sentiment".equals(this.mainSelectCol)) {
					main_item = EnumSentiment.getEnum(main_item).getName();
				}
				if ("sentiment".equals(this.secSelectCol)) {
					sec_item = EnumSentiment.getEnum(sec_item).getName();
				}
				int count = rs.getInt("count");
				int title_count = rs.getInt("title_count");
				int content_count = rs.getInt("content_count");
				int comment_count = rs.getInt("comment_count");
				LOGGER.debug("main_item=" + main_item + ", sec_item=" + sec_item + ", count=" + count + ", title_count: " + title_count + ", content_count: " + content_count + ", comment_count: " + comment_count);
				
				if (hash_mainItem_secItem.get(main_item) == null) {
					hash_mainItem_secItem.put(main_item, new HashMap<String, Integer>());
				}
				if (hash_mainItem_secItem_title.get(main_item) == null) {
					hash_mainItem_secItem_title.put(main_item, new HashMap<String, Integer>());
				}
				if (hash_mainItem_secItem_content.get(main_item) == null) {
					hash_mainItem_secItem_content.put(main_item, new HashMap<String, Integer>());
				}
				if (hash_mainItem_secItem_comment.get(main_item) == null) {
					hash_mainItem_secItem_comment.put(main_item, new HashMap<String, Integer>());
				}
				Map<String, Integer> secItemHM_count = hash_mainItem_secItem.get(main_item);
				Map<String, Integer> secItemHM_title = hash_mainItem_secItem_title.get(main_item);
				Map<String, Integer> secItemHM_content = hash_mainItem_secItem_content.get(main_item);
				Map<String, Integer> secItemHM_comment = hash_mainItem_secItem_comment.get(main_item);
				secItemHM_count.put(sec_item, count);
				hash_mainItem_secItem.put(main_item, secItemHM_count);
				secItemHM_title.put(sec_item, title_count);
				hash_mainItem_secItem_title.put(main_item, secItemHM_title);
				secItemHM_content.put(sec_item, content_count);
				hash_mainItem_secItem_content.put(main_item, secItemHM_content);
				secItemHM_comment.put(sec_item, comment_count);
				hash_mainItem_secItem_comment.put(main_item, secItemHM_comment);
			}
			LOGGER.debug("hash_mainItem_secItem=" + hash_mainItem_secItem);
			
			// Convert channel_id to channel_name; website_id to website_name:
			this.convertIdToName(this.mainValueArr, this.secValueArr);
			
			JSONArray resultArray = new JSONArray();
			for (String mainValue : mainValueArr) {
				Map<String, Integer> secItemHM_count = hash_mainItem_secItem.get(mainValue);
				Map<String, Integer> secItemHM_title = hash_mainItem_secItem_title.get(mainValue);
				Map<String, Integer> secItemHM_content = hash_mainItem_secItem_content.get(mainValue);
				Map<String, Integer> secItemHM_comment = hash_mainItem_secItem_comment.get(mainValue);
				JSONArray secItemArr = new JSONArray();
				for (String secValue : secValueArr) {
					Integer count = null;
					Integer title_count = null;
					Integer content_count = null;
					Integer comment_count = null;
					if (secItemHM_count != null) {
						count = secItemHM_count.get(secValue);
					}
					if (count == null) count = 0;
					
					if (secItemHM_title != null) {
						title_count = secItemHM_title.get(secValue);
					}
					if (title_count == null) title_count = 0;
					
					if (secItemHM_content != null) {
					content_count = secItemHM_content.get(secValue);
					}
					if (content_count == null) content_count = 0;
					
					if (secItemHM_comment != null) {
						comment_count = secItemHM_comment.get(secValue);
					}
					if (comment_count == null) comment_count = 0;
					
					JSONObject secItemObj = new JSONObject();
					secItemObj.put("sec_item", secValue);
					secItemObj.put("count", count);
					secItemObj.put("title_count", title_count);
					secItemObj.put("content_count", content_count);
					secItemObj.put("comment_count", comment_count);
					secItemArr.put(secItemObj);
				}
				JSONObject resultObj = new JSONObject();
				resultObj.put("main_item", mainValue);
				resultObj.put("data", secItemArr);
				resultObj.put("totalCount", this.getTotalCount(secItemArr)); // for sort later
				resultArray.put(resultObj);
			}
			
			JSONArray sortedResultArray = this.getSortedResultArray(resultArray, this.limit);
			return sortedResultArray;
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, preparedStatement, conn);
		}
		return null;
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
		        } 
		        catch (Exception e) {
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
	
	private Integer getTotalCount(JSONArray secItemArr) {
		Integer totalCount = 0;
		for (int i = 0; i < secItemArr.length(); i++) {
			JSONObject secItem = secItemArr.getJSONObject(i);
			totalCount += secItem.getInt("count");
		}
		return totalCount;
	}
	
	private void convertIdToName(String[] mainValueArr, String[] secValueArr) {
		if ("channel".equals(this.mainFilter)) {
			for (int i = 0; i < mainValueArr.length; i++) {
				String mainValue = mainValueArr[i];
				// mainValueArr[i] = this.getChannelNameById(this.tableName, mainValue);
				mainValueArr[i] = this.getChannelNameById(mainValue);
			}
		}
//		if ("website".equals(this.mainFilter)) {
//			for (int i = 0; i < mainValueArr.length; i++) {
//				String mainValue = mainValueArr[i];
//				// mainValueArr[i] = this.getWebsiteNameById(this.tableName, mainValue);
//				mainValueArr[i] = this.getWebsiteNameById(mainValue);
//			}
//		}
		if ("sentiment".equals(this.mainFilter)) {
			for (int i = 0; i < mainValueArr.length; i++) {
				String mainValue = mainValueArr[i];
				mainValueArr[i] = EnumSentiment.getEnum(mainValue).getName();
			}
		}
		if ("channel".equals(this.secFilter)) {
			for (int i = 0; i < secValueArr.length; i++) {
				String secValue = secValueArr[i];
				// secValueArr[i] = this.getChannelNameById(this.tableName, mainValue);
				secValueArr[i] = this.getChannelNameById(secValue);
			}
		}
//		if ("website".equals(this.secFilter)) {
//			for (int i = 0; i < secValueArr.length; i++) {
//				String secValue = secValueArr[i];
//				// secValueArr[i] = this.getWebsiteNameById(this.tableName, secValue);
//				secValueArr[i] = this.getWebsiteNameById(secValue);
//			}
//		}
		if ("sentiment".equals(this.secFilter)) {
			for (int i = 0; i < secValueArr.length; i++) {
				String secValue = secValueArr[i];
				secValueArr[i] = EnumSentiment.getEnum(secValue).getName();
			}
		}
	}

	private String genSelectSQL(String tableName, String mainFilterColumn, String secFilterColumn, String[] mainValueArr, String[] secValueArr, 
			String[] monitorBrandArr) {
		
		StringBuffer selectSQL = new StringBuffer();
		this.mainSelectCol = mainFilterColumn;
		if ("website_id".equals(mainFilterColumn)) {
			this.mainSelectCol = "website_name";
		} else if ("channel_id".equals(mainFilterColumn)) {
			this.mainSelectCol = "channel_display_name";
		}
		this.secSelectCol = secFilterColumn;
		if ("channel_id".equals(secFilterColumn)) {
			this.secSelectCol = "channel_display_name";
		}
		selectSQL.append("SELECT ").append(this.mainSelectCol).append(", ").append(this.secSelectCol).append(", ").append("SUM(reputation) AS count, SUM(title_hit) AS title_count, SUM(content_hit) AS content_count, SUM(comment_hit) AS comment_count ");
		selectSQL.append("FROM ").append(tableName).append(" ");
		selectSQL.append("WHERE ").append(mainFilterColumn).append(" IN (");
		for(int i = 0 ; i < mainValueArr.length ; i++ ) {
			if (i == 0) selectSQL.append("?");
			else selectSQL.append(",?");
		}
		selectSQL.append(") ");
		selectSQL.append("AND ").append(secFilterColumn).append(" IN (");
		for(int i = 0 ; i < secValueArr.length ; i++ ) {
			if (i == 0) selectSQL.append("?");
			else selectSQL.append(",?");
		}
		selectSQL.append(") ");
		
		if (StringUtils.isNotBlank(monitor_brand)) {
			selectSQL.append("AND brand IN (");
			for(int i = 0 ; i < monitorBrandArr.length ; i++ ) {
				if (i == 0) selectSQL.append("?");
				else selectSQL.append(",?");
			}
			selectSQL.append(") ");
		}
		
		selectSQL.append("AND DATE_FORMAT(rep_date, '%Y-%m-%d') >= ? ");
		selectSQL.append("AND DATE_FORMAT(rep_date, '%Y-%m-%d') <= ? ");
		selectSQL.append("GROUP BY ").append(mainFilterColumn).append(", ").append(secFilterColumn).append(" ");
		selectSQL.append("ORDER BY count DESC");
		return selectSQL.toString();
	}
	
	private void setWhereClauseValues(PreparedStatement preparedStatement, String[] mainValueArr, String[] secValueArr, 
			String[] monitorBrandArr, String startDate, String endDate) throws Exception {
		
		int idx = 0;
		for(String v : mainValueArr) {
			int parameterIndex = idx + 1;
			preparedStatement.setObject(parameterIndex, v);
			// LOGGER.debug(parameterIndex + ":" + v);
			idx++;
		}
		for(String v : secValueArr) {
			int parameterIndex = idx + 1;
			preparedStatement.setObject(parameterIndex, v);
			// LOGGER.debug(parameterIndex + ":" + v);
			idx++;
		}
		
		if (StringUtils.isNotBlank(monitor_brand)) {
			for(String v : monitorBrandArr) {
				int parameterIndex = idx + 1;
				preparedStatement.setObject(parameterIndex, v);
				// LOGGER.debug(parameterIndex + ":" + v);
				idx++;
			}
		}
		
		int startDateIndex = idx + 1;
		preparedStatement.setObject(startDateIndex, startDate);
		// LOGGER.debug(startDateIndex + ":" + startDate);
		idx++;
		
		int endDateIndex = idx + 1;
		preparedStatement.setObject(endDateIndex, endDate);
		// LOGGER.debug(endDateIndex + ":" + endDate);
		idx++;
	}
	
}
