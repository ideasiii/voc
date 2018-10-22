package com.voc.api.industry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.catalina.util.ParameterMap;
import org.json.JSONArray;
import org.json.JSONObject;

import com.voc.api.RootAPI;
import com.voc.common.DBUtil;
import com.voc.enums.industry.EnumTotalCount;

/**
 * 查詢產業分析口碑總數
 * /industry/total-count.jsp 
 *
 */
public class TotalCount extends RootAPI {

	/**
	 * 查詢產業分析口碑總數: 
	 * 
	 * EX-1: ibuzz_voc.product_reputation (產品表格)
	 * SELECT SUM(reputation) FROM ibuzz_voc.product_reputation WHERE series = '掀背車' and website_id = '5b29c821a85d0a7df5c3ff22' AND date >= '2018-05-01 00:00:00' AND date <= '2018-05-30 23:59:59';
	 * http://localhost:8080/voc/industry/total-count.jsp?series=掀背車&website=5b29c821a85d0a7df5c3ff22&start_date=2018-05-01 00:00:00&end_date=2018-05-30 23:59:59
	 * http://localhost:8080/voc/industry/total-count.jsp?series=掀背車&website=5b29c821a85d0a7df5c3ff22&start_date=2018-05-01&end_date=2018-05-30
	 * 
	 * SELECT brand, series, SUM(reputation) AS count FROM ibuzz_voc.product_reputation where brand in ('BENZ', 'BMW') and series in('掀背車', '轎車') and DATE_FORMAT(date, '%Y-%m-%d') >= '2018-01-01' AND DATE_FORMAT(date, '%Y-%m-%d') <= '2018-10-31' GROUP BY  brand, series;
	 * http://localhost:8080/voc/industry/total-count.jsp?brand=BENZ;BMW&series=掀背車;轎車&start_date=2018-01-01&end_date=2018-10-31
	 * 
	 * 
	 * EX-2: ibuzz_voc.brand_reputation (品牌表格)
	 * SELECT SUM(reputation) FROM ibuzz_voc.brand_reputation where brand = 'BENZ' and website_id = '5b29c824a85d0a7df5c40080' and date >= '2018-05-01 00:00:00' AND date <= '2018-05-02 23:59:59';
	 * http://localhost:8080/voc/industry/total-count.jsp?brand=BENZ&website=5b29c824a85d0a7df5c40080&start_date=2018-05-01 00:00:00&end_date=2018-05-02 23:59:59
	 * http://localhost:8080/voc/industry/total-count.jsp?brand=BENZ&website=5b29c824a85d0a7df5c40080&start_date=2018-05-01&end_date=2018-05-02
	 * 
	 * SELECT SUM(reputation) FROM ibuzz_voc.brand_reputation where brand in ('BENZ', 'BMW') and website_id = '5b29c824a85d0a7df5c40080' and date >= '2018-05-01 00:00:00' AND date <= '2018-05-02 23:59:59';
	 * http://localhost:8080/voc/industry/total-count.jsp?brand=BENZ;BMW&website=5b29c824a85d0a7df5c40080&start_date=2018-05-01&end_date=2018-05-02
	 * 
	 * SELECT SUM(reputation) FROM ibuzz_voc.brand_reputation where brand in ('BENZ', 'BMW') and website_id in ( '5b29c824a85d0a7df5c40080', '5b29c821a85d0a7df5c3ff22') and date >= '2018-05-01 00:00:00' AND date <= '2018-05-02 23:59:59';
	 * http://localhost:8080/voc/industry/total-count.jsp?brand=BENZ;BMW&website=5b29c824a85d0a7df5c40080;5b29c821a85d0a7df5c3ff22&start_date=2018-05-01&end_date=2018-05-02
	 * 
	 * SELECT brand, website_name, SUM(reputation) AS count FROM ibuzz_voc.brand_reputation where brand in ('BENZ', 'BMW') and website_id in('5b29c824a85d0a7df5c40080', '5b29c821a85d0a7df5c3ff22') and DATE_FORMAT(date, '%Y-%m-%d') >= '2018-05-01' AND DATE_FORMAT(date, '%Y-%m-%d') <= '2018-05-02' GROUP BY brand, website_id;
	 * http://localhost:8080/voc/industry/total-count.jsp?brand=BENZ;BMW&website=5b29c824a85d0a7df5c40080;5b29c821a85d0a7df5c3ff22&start_date=2018-05-01&end_date=2018-05-02
	 * http://localhost:8080/voc/industry/total-count.jsp?website=5b29c824a85d0a7df5c40080;5b29c821a85d0a7df5c3ff22&brand=BENZ;BMW&start_date=2018-05-01&end_date=2018-05-02
	 * 
	 * Note: 呼叫API時所下的參數若包含 product 或 series, 就使用 ibuzz_voc.product_reputation (產品表格), 否則就使用 ibuzz_voc.brand_reputation (品牌表格)
	 *       ==>See RootAPI.java
	 * 
	 */
	@Override
	public JSONObject processRequest(HttpServletRequest request) {
		Map<String, String[]> parameterMap = request.getParameterMap();
		parameterMap = this.adjustParameterOrder(parameterMap);
		
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		StringBuffer selectSQL = new StringBuffer();
		try {
			selectSQL.append(this.genSelectClause(parameterMap));
			selectSQL.append(this.genWhereClause(parameterMap));
			selectSQL.append(this.genGroupByOrderByClause(parameterMap));
			System.out.println("debug:==>" + selectSQL.toString()); // debug
			
			conn = DBUtil.getConn();
			preparedStatement = conn.prepareStatement(selectSQL.toString());
			this.setWhereClauseValues(preparedStatement, parameterMap);
			
			System.out.println("debug:=================================" ); // debug
			ResultSet rs = preparedStatement.executeQuery();
			JSONArray itemArray = new JSONArray();
			while (rs.next()) {
				StringBuffer item = new StringBuffer();
				int i = 0;
				for (Map.Entry<String, String[]> entry : parameterMap.entrySet()) {
					String paramName = entry.getKey();			
					String columnName = this.getColumnName(paramName);
					if ("website_id".equals(columnName)) {
						columnName = "website_name";
					} else if ("channel_id".equals(columnName)) {
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
				System.out.println("debug:==>item=" + item.toString() + ", count=" + count); // debug
				
				JSONObject itemObject = new JSONObject();
				itemObject.put("item", item.toString());
				itemObject.put("count", count);
				
				itemArray.put(itemObject);
			}
			
			JSONObject responseObject = new JSONObject();
			responseObject.put("success", true);
			responseObject.put("result", itemArray);
			return responseObject;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (conn != null) {
				DBUtil.closeConn(conn);
			}
		}
		return null;
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
			EnumTotalCount enumTotalCount = EnumTotalCount.getEnum(paramName);
			if (enumTotalCount == null) continue; 
			switch (enumTotalCount) {
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
			String paramName = EnumTotalCount.PARAM_COLUMN_INDUSTRY.getParamName();
			orderedParameterMap.put(paramName, paramValues_industry);
		}
		if (paramValues_brand != null) {
			String paramName = EnumTotalCount.PARAM_COLUMN_BRAND.getParamName();
			orderedParameterMap.put(paramName, paramValues_brand);
		}
		if (paramValues_series != null) {
			String paramName = EnumTotalCount.PARAM_COLUMN_SERIES.getParamName();
			orderedParameterMap.put(paramName, paramValues_series);
		}
		if (paramValues_product != null) {
			String paramName = EnumTotalCount.PARAM_COLUMN_PRODUCT.getParamName();
			orderedParameterMap.put(paramName, paramValues_product);
		}
		if (paramValues_source != null) {
			String paramName = EnumTotalCount.PARAM_COLUMN_SOURCE.getParamName();
			orderedParameterMap.put(paramName, paramValues_source);
		}
		if (paramValues_website != null) {
			String paramName = EnumTotalCount.PARAM_COLUMN_WEBSITE.getParamName();
			orderedParameterMap.put(paramName, paramValues_website);
		}
		if (paramValues_channel != null) {
			String paramName = EnumTotalCount.PARAM_COLUMN_CHANNEL.getParamName();
			orderedParameterMap.put(paramName, paramValues_channel);
		}
		if (paramValues_features != null) {
			String paramName = EnumTotalCount.PARAM_COLUMN_FEATURES.getParamName();
			orderedParameterMap.put(paramName, paramValues_features);
		}
		if (paramValues_startDate != null) {
			String paramName = EnumTotalCount.PARAM_COLUMN_START_DATE.getParamName();
			orderedParameterMap.put(paramName, paramValues_startDate);
		}
		if (paramValues_endDate != null) {
			String paramName = EnumTotalCount.PARAM_COLUMN_END_DATE.getParamName();
			orderedParameterMap.put(paramName, paramValues_endDate);
		}
		return orderedParameterMap;
	}
	
	private String genSelectClause(Map<String, String[]> parameterMap) {
		StringBuffer selectClauseSB = new StringBuffer();
		selectClauseSB.append("SELECT ");
		int i = 0;
		for (Map.Entry<String, String[]> entry : parameterMap.entrySet()) {
			String paramName = entry.getKey();
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
		String tableName = this.getTableName(parameterMap);
		selectClauseSB.append(" ,").append("SUM(reputation) AS count FROM ").append(tableName).append(" ");
		return selectClauseSB.toString();
	}
	
	private String genWhereClause(Map<String, String[]> parameterMap) {
		StringBuffer whereClauseSB = new StringBuffer();
		int i = 0;
		for (Map.Entry<String, String[]> entry : parameterMap.entrySet()) {
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
	
	private String genGroupByOrderByClause(Map<String, String[]> parameterMap) {
		StringBuffer groupByOrderByClauseSB = new StringBuffer();
		StringBuffer columnsSB = new StringBuffer();
		int i = 0;
		for (Map.Entry<String, String[]> entry : parameterMap.entrySet()) {
			String paramName = entry.getKey();			
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
		groupByOrderByClauseSB.append("ORDER BY ").append(columnsSB.toString());
		return groupByOrderByClauseSB.toString();
	}

	private void setWhereClauseValues(PreparedStatement preparedStatement, Map<String, String[]> parameterMap) throws Exception {
		int i = 0;
		for (Map.Entry<String, String[]> entry : parameterMap.entrySet()) {
			String paramName = entry.getKey();
			String[] values = entry.getValue();
			String value = "";
			if (values != null && values.length > 0) {
				value = values[0];
				if (paramName.equals("start_date") || paramName.equals("end_date")) {
					int parameterIndex = i + 1;
					preparedStatement.setObject(parameterIndex, value);
					System.out.println("debug:==>" + parameterIndex + ":" + value); // debug
					i++;
				} else {
					String[] valueArr = entry.getValue()[0].split(PARAM_VALUES_SEPARATOR);
					for (String v : valueArr) {
						int parameterIndex = i + 1;
						preparedStatement.setObject(parameterIndex, v);
						System.out.println("debug:==>" + parameterIndex + ":" + v); // debug
						i++;
					}
				}
			}
		}
	}

}
