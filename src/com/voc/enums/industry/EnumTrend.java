package com.voc.enums.industry;

import java.util.HashMap;
import java.util.Map;

public enum EnumTrend {
	PARAM_COLUMN_INDUSTRY("industry", "industry"),
	PARAM_COLUMN_BRAND("brand", "brand"),
	PARAM_COLUMN_SERIES("series", "series"),
	PARAM_COLUMN_PRODUCT("product", "product"), 
	PARAM_COLUMN_SOURCE("source", "source"),
	PARAM_COLUMN_WEBSITE("website", "website_name"),
	PARAM_COLUMN_CHANNEL("channel", "channel_id"),
	PARAM_COLUMN_FEATURES("features", "features"),
	PARAM_COLUMN_START_DATE("start_date", "date"),
	PARAM_COLUMN_END_DATE("end_date", "date");

	private final String paramName;
	private final String columnName;
	
	private static Map<String, EnumTrend> mapper;
	
	private EnumTrend(String paramName, String columnName) {
		this.paramName = paramName;
		this.columnName = columnName;
	}

	public String getParamName() {
		return paramName;
	}

	public String getColumnName() {
		return columnName;
	}

	public static EnumTrend getEnum(String paramName){
        if(mapper == null){
        	mapper = new HashMap<String, EnumTrend>();
            for(EnumTrend obj : EnumTrend.values()){
            	mapper.put(obj.getParamName(), obj);
            }
        }
        return mapper.get(paramName);
    }

}
