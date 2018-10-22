package com.voc.api.industry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.json.JSONObject;

import com.voc.api.RootAPI;
import com.voc.common.DBUtil;

/**
 * 查詢產業分析交叉分析口碑總數:
 * - 查詢時間區間內各項目交叉分析的口碑總數及比例
 * - /industry/cross-ratio.jsp
 *
 */
public class CrossRatio extends RootAPI {

	/**
	 * 查詢時間區間內各項目交叉分析的口碑總數及比例:
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
	 * SELECT brand, website_name, SUM(reputation) AS count FROM ibuzz_voc.brand_reputation WHERE brand in ('BENZ', 'BMW') and website_id in('5b29c824a85d0a7df5c40080', '5b29c821a85d0a7df5c3ff22') and DATE_FORMAT(date, '%Y-%m-%d') >= '2018-05-01' AND DATE_FORMAT(date, '%Y-%m-%d') <= '2018-05-02' GROUP BY brand, website_id;
	 * http://localhost:8080/voc/industry/cross-ratio.jsp?main-filter=brand&main-value=BENZ;BMW&sec-filter=website&sec-value=5b29c824a85d0a7df5c40080;5b29c821a85d0a7df5c3ff22&start_date=2018-05-01&end_date=2018-05-02
	 * 
	 * 
	 */
	@Override
	public JSONObject processRequest(HttpServletRequest request) {
		String mainFilter = request.getParameter("main-filter");
		String mainValue = request.getParameter("main-value");
		String secFilter = request.getParameter("sec-filter");
		String secValue = request.getParameter("sec-value");
		String startDate = request.getParameter("start_date");
		String endDate = request.getParameter("end_date");
		
		// TODO: param validation: 
		
		String mainFilterColumn = this.getColumnName(mainFilter);
		String secFilterColumn = this.getColumnName(secFilter);
		
		List<String> paramNameList = new ArrayList<>();
		paramNameList.add(mainFilter);
		paramNameList.add(secFilter);
		String tableName = this.getTableName(paramNameList);
		
		Connection conn = null;
		PreparedStatement preparedStatement = null;
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
			
			JSONObject responseObj = new JSONObject();
			ResultSet rs = preparedStatement.executeQuery();
			int i = 1;
			while (rs.next()) {
				
				// TODO: 
				
				int count = rs.getInt("count");
				System.out.println("debug:==>count=" + count); // debug
				
				responseObj.put("still_working_" + i, count);
				i++;
			}
			
			// TODO: 
			return responseObj;
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (conn != null) {
				DBUtil.closeConn(conn);
			}
		}
		return null;
	}
	
	private String genSelectSQL(String tableName, String mainFilterColumn, String secFilterColumn, String[] mainValueArr, String[] secValueArr) {
		StringBuffer selectSQL = new StringBuffer();
		selectSQL.append("SELECT ").append(mainFilterColumn).append(", ").append(secFilterColumn).append(", ").append("SUM(reputation) AS count ");
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
