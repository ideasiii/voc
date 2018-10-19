package com.voc.api.industry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;

import org.json.JSONArray;
import org.json.JSONObject;

import com.voc.api.RootAPI;
import com.voc.common.ApiResponse;
import com.voc.common.Common;
import com.voc.common.DBUtil;

public class Trend extends RootAPI {

	@Override
	public JSONObject processRequest(HttpServletRequest request) {

		Map<String, String[]> paramMap = request.getParameterMap();

		if (!hasRequiredParameters(paramMap)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}

		final String strStartDate = request.getParameter("start_date");
		final String strEndDate = request.getParameter("end_date");
		String strInterval;

		if (!Common.isValidDate(strStartDate, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid start_date.");
		}

		if (!Common.isValidDate(strEndDate, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid end_date.");
		}

		if (!Common.isValidStartDate(strStartDate, strEndDate, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid period values.");
		}

		if (!hasInterval(paramMap)) {
			strInterval = Common.INTERVAL_DAILY;
		} else {
			strInterval = request.getParameter("interval");
			if (!Common.isValidInterval(strInterval)) {
				return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid interval.");
			}
		}

		String strSelectClause = genSelectClause(paramMap, strInterval);
		String strWhereClause = genWhereClause(paramMap, strInterval);

		System.out.println("**************SQL: " + strSelectClause + strWhereClause);

		JSONObject jobj = new JSONObject();
		JSONArray resArray = new JSONArray();
		int nCount = query(paramMap, strSelectClause, strWhereClause, resArray);

		if (0 < nCount) {
			jobj = ApiResponse.successTemplate();
			jobj.put("result", resArray);

		} else {
			switch (nCount) {
			case 0:
				jobj = ApiResponse.dataNotFound();
				break;
			default:
				jobj = ApiResponse.byReturnStatus(nCount);
			}
		}
		return jobj;
	}

	private int query(Map<String, String[]> paramMap, final String strSelectClause, final String strWhereClause,
			final JSONArray out) {
		Connection conn = null;
		PreparedStatement pst = null;
		StringBuffer querySQL = new StringBuffer();
		int status = 0;
		try {
			querySQL.append(strSelectClause);
			querySQL.append(strWhereClause);
			System.out.println("****************" + querySQL.toString());

			conn = DBUtil.getConn();
			pst = conn.prepareStatement(querySQL.toString());
			setWhereClauseValues(pst, paramMap);

			ResultSet rs = pst.executeQuery();
			while (rs.next()) {
				JSONObject jobj = new JSONObject();
				int count = rs.getInt("count");
				jobj.put("count", count);
				out.put(jobj);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (conn != null) {
				DBUtil.closeConn(conn);
			}
		}
		return status;
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
		String strTableName = getTableName(paramMap);
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
				String[] arrValue = entry.getValue();
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
		if (strInterval.equals(Common.INTERVAL_DAILY)) {
			sql.append(" GROUP BY DATE_FORMAT(date, '%Y-%m-%d')");
		} else if (strInterval.equals(Common.INTERVAL_MONTHLY)) {
			sql.append(" GROUP BY DATE_FORMAT(date, '%Y-%m')");
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
					System.out.println("*********" + parameterIndex + ":" + value); 
					i++;
				} else {
					String[] valueArr = entry.getValue();
					for (String v : valueArr) {
						int parameterIndex = i + 1;
						pst.setObject(parameterIndex, v);
						System.out.println("*********" + parameterIndex + ":" + v);
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

}