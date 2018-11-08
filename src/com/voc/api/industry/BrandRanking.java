package com.voc.api.industry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.catalina.util.ParameterMap;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voc.api.RootAPI;
import com.voc.common.ApiResponse;
import com.voc.common.Common;
import com.voc.common.DBUtil;

public class BrandRanking extends RootAPI {
	private static final Logger LOGGER = LoggerFactory.getLogger(Trend.class);
	private String strSort;
	private String strLimit;

	@Override
	public JSONObject processRequest(HttpServletRequest request) {

		Map<String, String[]> paramMap = request.getParameterMap();
		strSort = request.getParameter("sort");
		strLimit = request.getParameter("limit");
		
		if (!hasRequiredParameters(paramMap)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}
		
		JSONArray resArray = query(paramMap);
		
		if (null != resArray) {
			JSONObject jobj = ApiResponse.successTemplate();
			jobj.put("result", resArray);
			LOGGER.info("response: "+ jobj.toString());
			return jobj;
		}
		return ApiResponse.unknownError();
	}
	
	private JSONArray query(Map<String, String[]> paramMap) {
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		String strTableName = getTableName(paramMap);
		StringBuffer querySQL = new StringBuffer();
		try {
			querySQL.append("SELECT brand, SUM(reputation) AS count FROM ").append(strTableName).append(" ");
			querySQL.append(genWhereClause(paramMap));
			querySQL.append(" GROUP BY brand");
			querySQL.append(sort());
			querySQL.append(limit());
			LOGGER.info("querySQL: " + querySQL.toString());
			
			conn = DBUtil.getConn();
			pst = conn.prepareStatement(querySQL.toString());
			rs = pst.executeQuery();
			
			JSONArray out = new JSONArray();
			while (rs.next()) {
				JSONObject jobj = new JSONObject();
				jobj.put("brand", rs.getString("brand"));
				jobj.put("count", rs.getInt("count"));
				out.put(jobj);
			}
			return out;
			
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, pst, conn);
		}
		return null;
	}
	
	private String genWhereClause(Map<String, String[]> paramMap) {
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
	

	//private boolean validate() {
		
	//}
	
	private String sort() {
		if (null == strSort) {
		strSort = Common.SORT_DESC;
		}
		StringBuffer sql = new StringBuffer();
		sql.append(" ORDER BY ").append(strSort);
	return sql.toString();	
	}
	
	private String limit() {
		int nLimit = 0;
		if (null == strLimit) {
		nLimit = 10;
		} else {
			nLimit = Integer.parseInt(strLimit);
		}
		StringBuffer sql = new StringBuffer();
		sql.append(" LIMIT ").append(nLimit);
		return sql.toString();	
	}
	

	private boolean hasRequiredParameters(Map<String, String[]> paramMap) {
		return paramMap.containsKey("brand") && paramMap.containsKey("start_date") && paramMap.containsKey("end_date");
	}
	
}
