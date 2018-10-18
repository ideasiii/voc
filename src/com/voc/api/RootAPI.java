package com.voc.api;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.json.JSONObject;

public abstract class RootAPI {
	private static final String TABLE_PRODUCT_REPUTATION = "ibuzz_voc.product_reputation";
	private static final String TABLE_BRAND_REPUTATION = "ibuzz_voc.brand_reputation";
	private static final Map<String, String> PARAM_COLUMN_MAP = new HashMap<String, String>();
	static {
		PARAM_COLUMN_MAP.put("industry", "industry");
		PARAM_COLUMN_MAP.put("brand", "brand");
		PARAM_COLUMN_MAP.put("series", "series");
		PARAM_COLUMN_MAP.put("product", "product_name");
		PARAM_COLUMN_MAP.put("source", "source");
		PARAM_COLUMN_MAP.put("website", "website_id");
		PARAM_COLUMN_MAP.put("channel", "channel_id");
		PARAM_COLUMN_MAP.put("features", "features");
		PARAM_COLUMN_MAP.put("start_date", "date");
		PARAM_COLUMN_MAP.put("end_date", "date");
	}
	protected static final String PARAM_VALUES_SEPARATOR = ";";

	public abstract JSONObject processRequest(HttpServletRequest request);
	
	protected String getTableName(Map<String, String[]> parameterMap) {
		if (parameterMap.containsKey("product") || parameterMap.containsKey("series")) {
			return TABLE_PRODUCT_REPUTATION;
		} else {
			return TABLE_BRAND_REPUTATION;
		}
	}
	
	protected String getColumnName(String paramName) {
		return PARAM_COLUMN_MAP.get(paramName);
	}
	

}
