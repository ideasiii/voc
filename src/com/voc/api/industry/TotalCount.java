package com.voc.api.industry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.json.JSONObject;

import com.voc.api.RootAPI;
import com.voc.common.DBUtil;

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
	 * 
	 * EX-2: ibuzz_voc.brand_reputation (品牌表格)
	 * SELECT SUM(reputation) FROM ibuzz_voc.brand_reputation where brand = 'BENZ' and website_id = '5b29c824a85d0a7df5c40080' and date >= '2018-05-01 00:00:00' AND date <= '2018-05-02 23:59:59';
	 * http://localhost:8080/voc/industry/total-count.jsp?brand=BENZ&website=5b29c824a85d0a7df5c40080&start_date=2018-05-01 00:00:00&end_date=2018-05-02 23:59:59
	 * http://localhost:8080/voc/industry/total-count.jsp?brand=BENZ&website=5b29c824a85d0a7df5c40080&start_date=2018-05-01&end_date=2018-05-02
	 * 
	 * 
	 * Note: 呼叫API時所下的參數若包含 product 或 series, 就使用 ibuzz_voc.product_reputation (產品表格), 否則就使用 ibuzz_voc.brand_reputation (品牌表格)
	 *       ==>See RootAPI.java
	 * 
	 */
	@Override
	public JSONObject processRequest(HttpServletRequest request) {
		Map<String, String[]> parameterMap = request.getParameterMap();
		
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		StringBuffer selectSQL = new StringBuffer();
		try {
			String tableName = this.getTableName(parameterMap);
			selectSQL.append("SELECT SUM(reputation) AS count FROM ").append(tableName).append(" ");
			selectSQL.append(this.getWhereClause(parameterMap));
			System.out.println("debug:==>\r\n" + selectSQL.toString()); // debug
			
			conn = DBUtil.getConn();
			preparedStatement = conn.prepareStatement(selectSQL.toString());
			this.setWhereClauseValues(preparedStatement, parameterMap);
			
			ResultSet rs = preparedStatement.executeQuery();
			if (rs.next()) {
				int count = rs.getInt("count");
				JSONObject resultObject = new JSONObject();
				resultObject.put("count", count);
				
				JSONObject responseObject = new JSONObject();
				responseObject.put("success", true);
				responseObject.put("result", resultObject);
				return responseObject;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (conn != null) {
				DBUtil.closeConn(conn);
			}
		}
		return null;
	}
	
	private String getWhereClause(Map<String, String[]> parameterMap) {
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
			whereClauseSB.append(columnName);
			if (paramName.equals("start_date")) {
				whereClauseSB.append(" >= ? ");
			} else if (paramName.equals("end_date")) {
				whereClauseSB.append(" <= ? ");
			} else {
				whereClauseSB.append(" = ? ");
			}
			i++;
		}
		return whereClauseSB.toString();
	}

	private void setWhereClauseValues(PreparedStatement preparedStatement, Map<String, String[]> parameterMap) throws Exception {
		int i = 0;
		for (Map.Entry<String, String[]> entry : parameterMap.entrySet()) {
			String paramName = entry.getKey();
			String[] values = entry.getValue();
			String value = "";
			if (values != null && values.length > 0) {
				value = values[0];
				if (paramName.equals("start_date")) {
					value += " 00:00:00";
				} else if (paramName.equals("end_date")) {
					value += " 23:59:59";
				}
			}
			int parameterIndex = i + 1;
			preparedStatement.setObject(parameterIndex, value);
			System.out.println("debug:==>" + parameterIndex + ":" + value); // debug
			i++;
		}
	}

}
