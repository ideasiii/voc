package com.voc.api;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.json.JSONObject;

public abstract class RootAPI {
	private static final String TABLE_PRODUCT_REPUTATION = "ibuzz_voc.product_reputation";
	private static final String TABLE_BRAND_REPUTATION = "ibuzz_voc.brand_reputation";
	private static final Map<String, String> paramColumnMap = new HashMap<String, String>();
	static {
		paramColumnMap.put("industry", "industry");
		paramColumnMap.put("brand", "brand");
		paramColumnMap.put("series", "series");
		paramColumnMap.put("product", "product_name");
		paramColumnMap.put("source", "source");
		paramColumnMap.put("website", "website_id");
		paramColumnMap.put("channel", "channel_id");
		paramColumnMap.put("features", "features");
		paramColumnMap.put("start_date", "date");
		paramColumnMap.put("end_date", "date");
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
		return paramColumnMap.get(paramName);
	}
	

}
