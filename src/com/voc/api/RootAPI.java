package com.voc.api;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

import com.voc.common.DBUtil;

public abstract class RootAPI {
	public static final String API_KEY = "api_key";
	protected static final String UPDATE_TIME = "update_time";
	protected static final String PARAM_VALUES_SEPARATOR = ";";
	private static final String TABLE_PRODUCT_REPUTATION = "ibuzz_voc.product_reputation";
	private static final String TABLE_BRAND_REPUTATION = "ibuzz_voc.brand_reputation";
	private static final Map<String, String> PARAM_COLUMN_MAP = new HashMap<String, String>();
	static {
		PARAM_COLUMN_MAP.put("industry", "industry");
		PARAM_COLUMN_MAP.put("brand", "brand");
		PARAM_COLUMN_MAP.put("series", "series");
		PARAM_COLUMN_MAP.put("product", "product");
		PARAM_COLUMN_MAP.put("source", "source");
		PARAM_COLUMN_MAP.put("website", "website_id");
		PARAM_COLUMN_MAP.put("channel", "channel_id");
		PARAM_COLUMN_MAP.put("features", "features");
		PARAM_COLUMN_MAP.put("start_date", "date");
		PARAM_COLUMN_MAP.put("end_date", "date");
	}
	private static final List<String> VALID_PARAM_NAMES = Arrays.asList("industry", "brand", "series", "product",
			"source", "website", "channel", "features", "start_date", "end_date", "interval");
	
	private static final List<String> ITEM_PARAM_NAMES = Arrays.asList("industry", "brand", "series", "product",
			"source", "website", "channel", "features");

	public abstract JSONObject processRequest(HttpServletRequest request);

	protected String getTableName(Map<String, String[]> parameterMap) {
		if (parameterMap.containsKey("product") || parameterMap.containsKey("series")) {
			return TABLE_PRODUCT_REPUTATION;
		} else {
			return TABLE_BRAND_REPUTATION;
		}
	}
	
	protected String getTableName(List<String> paramNameList) {
		if (paramNameList.contains("product") || paramNameList.contains("series")) {
			return TABLE_PRODUCT_REPUTATION;
		} else {
			return TABLE_BRAND_REPUTATION;
		}
	}

	protected String getColumnName(String paramName) {
		return PARAM_COLUMN_MAP.get(paramName);
	}

	protected boolean isValidParamName(String paramName) {
		return VALID_PARAM_NAMES.contains(paramName);
	}

	protected boolean isItemParamName(String paramName) {
		return ITEM_PARAM_NAMES.contains(paramName);
	}
	
	protected String queryUpdateTime(String selectUpdateTimeSQL) {
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		String update_time = null;
		try {
			conn = DBUtil.getConn();
			preparedStatement = conn.prepareStatement(selectUpdateTimeSQL);
			rs = preparedStatement.executeQuery();
			if (rs.next()) {
				update_time = rs.getString(UPDATE_TIME);
			}
			return StringUtils.trimToEmpty(update_time);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, preparedStatement, conn);
		}
		return null;
	}
	
	protected String getChannelNameById(String channelId) {
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		String channelName = null;
		String selectSql = "SELECT channel_name FROM ibuzz_voc.product_reputation WHERE channel_id = ? LIMIT 1";
		try {
			conn = DBUtil.getConn();
			preparedStatement = conn.prepareStatement(selectSql);
			preparedStatement.setString(1, channelId);
			rs = preparedStatement.executeQuery();
			if (rs.next()) {
				channelName = rs.getString("channel_name");
			}
			return channelName;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, preparedStatement, conn);
		}
		return null;
	}
	
	protected String getWebsiteNameById(String websiteId) {
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		String websiteName = null;
		String selectSql = "SELECT website_name FROM ibuzz_voc.product_reputation WHERE website_id = ? LIMIT 1";
		try {
			conn = DBUtil.getConn();
			preparedStatement = conn.prepareStatement(selectSql);
			preparedStatement.setString(1, websiteId);
			rs = preparedStatement.executeQuery();
			if (rs.next()) {
				websiteName = rs.getString("website_name");
			}
			return websiteName;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, preparedStatement, conn);
		}
		return null;
	}

}
