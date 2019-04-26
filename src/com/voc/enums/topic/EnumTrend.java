package com.voc.enums.topic;

import java.util.HashMap;
import java.util.Map;

public enum EnumTrend {
	PARAM_COLUMN_USER("user", "user"),
	PARAM_COLUMN_TOPIC("topic", "topic"),
	PARAM_COLUMN_PROJECT("project_name", "project_name"),
	PARAM_COLUMN_SOURCE("source", "source"),
	PARAM_COLUMN_WEBSITE("website", "website_name"),
	PARAM_COLUMN_CHANNEL("channel", "channel_id"),
	PARAM_COLUMN_SENTIMENT("sentiment", "sentiment"),
	PARAM_COLUMN_START_DATE("start_date", "rep_date"),
	PARAM_COLUMN_END_DATE("end_date", "rep_date");

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
