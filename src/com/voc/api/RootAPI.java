package com.voc.api;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.json.JSONObject;

public abstract class RootAPI {
	private static final String TABLE_PRODUCT_REPUTATION = "ibuzz_voc.product_reputation";
	private static final String TABLE_BRAND_REPUTATION = "ibuzz_voc.brand_reputation";

	public abstract JSONObject processRequest(HttpServletRequest request);
	
	protected String getTableName(Map<String, String[]> parameterMap) {
		if (parameterMap.containsKey("product") || parameterMap.containsKey("series")) {
			return TABLE_PRODUCT_REPUTATION;
		} else {
			return TABLE_BRAND_REPUTATION;
		}
	}
	

}
