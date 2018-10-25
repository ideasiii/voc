package com.voc.api.industry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import com.voc.api.RootAPI;
import com.voc.common.ApiResponse;
import com.voc.common.Common;
import com.voc.common.DBUtil;

/**
 * 查詢產業分析交叉分析口碑總數:
 * - 查詢時間區間內各項目交叉分析的口碑總數及比例
 * - /industry/cross-ratio.jsp
 * 
 * Params:
 * query	*main-filter	string	主要分析項目
 * query	*main-value		string	主要欲分析項目值
 * query	*sec-filter		string	次要分析項目
 * query	*sec-value		string	次要欲分析項目值
 * query	*start_date		string	欲查詢起始日期
 * query	*end_date		string	欲查詢結束日期
 * 
 * 
 * SELECT brand, website_name, SUM(reputation) AS count FROM ibuzz_voc.brand_reputation WHERE brand IN ('BENZ', 'BMW') AND website_id IN('5b29c824a85d0a7df5c40080', '5b29c821a85d0a7df5c3ff22') AND DATE_FORMAT(date, '%Y-%m-%d') >= '2018-05-01' AND DATE_FORMAT(date, '%Y-%m-%d') <= '2018-05-02' GROUP BY brand, website_id;
 * http://localhost:8080/voc/industry/cross-ratio.jsp?main-filter=brand&main-value=BENZ;BMW&sec-filter=website&sec-value=5b29c824a85d0a7df5c40080;5b29c821a85d0a7df5c3ff22&start_date=2018-05-01&end_date=2018-05-02
 * 
 * 
 */
public class CrossRatio extends RootAPI {
	private String mainFilter = null;
	private String mainValue = null;
	private String secFilter = null;
	private String secValue = null;
	private String startDate = null;
	private String endDate = null;
	
	private String mainSelectCol = null;
	private String secSelectCol = null;
	
	private JSONObject validateParams() {
		if (StringUtils.isBlank(this.mainFilter) || StringUtils.isBlank(this.mainValue)
				|| StringUtils.isBlank(this.secFilter) || StringUtils.isBlank(this.secValue)
				|| StringUtils.isBlank(this.startDate) || StringUtils.isBlank(this.endDate)) {

			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}
		
		if (!Common.isValidDate(this.startDate, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid start_date.");
		}
		if (!Common.isValidDate(this.endDate, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid end_date.");
		}
		if (!Common.isValidStartDate(this.startDate, this.endDate, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid period values.");
		}
		return null;
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
	
	@Override
	public JSONObject processRequest(HttpServletRequest request) {
		this.mainFilter = StringUtils.trimToEmpty(request.getParameter("main-filter"));
		this.mainValue = StringUtils.trimToEmpty(request.getParameter("main-value"));
		this.secFilter = StringUtils.trimToEmpty(request.getParameter("sec-filter"));
		this.secValue = StringUtils.trimToEmpty(request.getParameter("sec-value"));
		this.startDate = StringUtils.trimToEmpty(request.getParameter("start_date"));
		this.endDate = StringUtils.trimToEmpty(request.getParameter("end_date"));
		
		this.mainValue = this.trimValues(this.mainValue);
		this.secValue = this.trimValues(this.secValue);
		
		JSONObject errorResponse = this.validateParams();
		if (errorResponse != null) {
			return errorResponse;
		}
		
		String mainFilterColumn = this.getColumnName(mainFilter);
		String secFilterColumn = this.getColumnName(secFilter);
		
		List<String> paramNameList = new ArrayList<>();
		paramNameList.add(mainFilter);
		paramNameList.add(secFilter);
		String tableName = this.getTableName(paramNameList);
		
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		StringBuffer selectSQL = new StringBuffer();
		try {
			String[] mainValueArr = mainValue.split(PARAM_VALUES_SEPARATOR);
			String[] secValueArr = secValue.split(PARAM_VALUES_SEPARATOR);
			selectSQL.append(this.genSelectSQL(tableName, mainFilterColumn, secFilterColumn, mainValueArr, secValueArr));
			System.out.println("debug:==>" + selectSQL.toString()); // debug
			
			conn = DBUtil.getConn();
			preparedStatement = conn.prepareStatement(selectSQL.toString());
			this.setWhereClauseValues(preparedStatement, mainValueArr, secValueArr, startDate, endDate);
			System.out.println("debug:=================================" ); // debug
			
			Map<String, JSONArray> mainItem_secItemArray_map = new HashMap<>();
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				String main_item = rs.getString(this.mainSelectCol);
				String sec_item = rs.getString(this.secSelectCol);
				int count = rs.getInt("count");
				System.out.println("debug:==>main_item=" + main_item + ", sec_item=" + sec_item + ", count=" + count); // debug
				
				JSONObject secItemObj = new JSONObject();
				secItemObj.put("sec_item", sec_item);
				secItemObj.put("count", count);
				
				if (mainItem_secItemArray_map.get(main_item) == null) {
					mainItem_secItemArray_map.put(main_item, new JSONArray());
				}
				JSONArray secItemArray = mainItem_secItemArray_map.get(main_item);
				secItemArray.put(secItemObj);
				mainItem_secItemArray_map.put(main_item, secItemArray);
			}
			
			JSONArray resultArray = new JSONArray();
			for (Map.Entry<String, JSONArray> entry : mainItem_secItemArray_map.entrySet()) {
				String main_item = entry.getKey();
				JSONArray secItemArray = entry.getValue();
				
				JSONObject resultObj = new JSONObject();
				resultObj.put("main_item", main_item);
				resultObj.put("data", secItemArray);

				resultArray.put(resultObj);
			}
			
			JSONObject responseObj = new JSONObject();
			responseObj.put("success", true);
			responseObj.put("result", resultArray);
			return responseObj;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				DBUtil.closeResultSet(rs);
			}
			if (preparedStatement != null) {
				DBUtil.closePreparedStatement(preparedStatement);
			}
			if (conn != null) {
				DBUtil.closeConn(conn);
			}
		}
		return ApiResponse.unknownError();
	}
	
	private String genSelectSQL(String tableName, String mainFilterColumn, String secFilterColumn, String[] mainValueArr, String[] secValueArr) {
		StringBuffer selectSQL = new StringBuffer();
		this.mainSelectCol = mainFilterColumn;
		if ("website_id".equals(mainFilterColumn)) {
			this.mainSelectCol = "website_name";
		} else if ("channel_id".equals(mainFilterColumn)) {
			this.mainSelectCol = "channel_name";
		}
		this.secSelectCol = secFilterColumn;
		if ("website_id".equals(secFilterColumn)) {
			this.secSelectCol = "website_name";
		} else if ("channel_id".equals(secFilterColumn)) {
			this.secSelectCol = "channel_name";
		}
		selectSQL.append("SELECT ").append(this.mainSelectCol).append(", ").append(this.secSelectCol).append(", ").append("SUM(reputation) AS count ");
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
		selectSQL.append("AND DATE_FORMAT(date, '%Y-%m-%d') >= ? ");
		selectSQL.append("AND DATE_FORMAT(date, '%Y-%m-%d') <= ? ");
		selectSQL.append("GROUP BY ").append(mainFilterColumn).append(", ").append(secFilterColumn).append(" ");
		selectSQL.append("ORDER BY ").append(mainFilterColumn).append(", ").append(secFilterColumn);
		return selectSQL.toString();
	}
	
	private void setWhereClauseValues(PreparedStatement preparedStatement, String[] mainValueArr, String[] secValueArr, String startDate, String endDate) throws Exception {
		int idx = 0;
		for(String v : mainValueArr) {
			int parameterIndex = idx + 1;
			preparedStatement.setObject(parameterIndex, v);
			System.out.println("debug:==>" + parameterIndex + ":" + v); // debug
			idx++;
		}
		for(String v : secValueArr) {
			int parameterIndex = idx + 1;
			preparedStatement.setObject(parameterIndex, v);
			System.out.println("debug:==>" + parameterIndex + ":" + v); // debug
			idx++;
		}
		
		int startDateIndex = idx + 1;
		preparedStatement.setObject(startDateIndex, startDate);
		System.out.println("debug:==>" + startDateIndex + ":" + startDate); // debug
		idx++;
		
		int endDateIndex = idx + 1;
		preparedStatement.setObject(endDateIndex, endDate);
		System.out.println("debug:==>" + endDateIndex + ":" + endDate); // debug
		idx++;
	}
	
}
