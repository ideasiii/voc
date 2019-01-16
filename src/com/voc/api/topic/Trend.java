package com.voc.api.topic;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.catalina.util.ParameterMap;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voc.api.RootAPI;
import com.voc.common.Common;

public class Trend extends RootAPI {
	private static final Logger LOGGER = LoggerFactory.getLogger(Trend.class);
	Map<String, String[]> orderedParameterMap = new ParameterMap<>();
	private List<String> itemNameList = new ArrayList<>();
	private JSONArray sortedJsonArray;
	private String selectUpdateTimeSQL;
	private String strTableName;
	private String strInterval = Common.INTERVAL_DAILY;
	private int limit = 10; // Default: 10

	@Override
	public String processRequest(HttpServletRequest request) {
		
		Map<String, String[]> paramMap = request.getParameterMap();
		strTableName = getTableName(paramMap);
		
		
		
		
		return null;
	}

}



