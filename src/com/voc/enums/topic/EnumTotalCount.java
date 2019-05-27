package com.voc.enums.topic;

import java.util.HashMap;
import java.util.Map;

public enum EnumTotalCount {
	PARAM_COLUMN_USER("user", "user"),
	PARAM_COLUMN_PROJECT_NAME("project_name", "project_name"),
	PARAM_COLUMN_TOPIC("topic", "topic"),
	PARAM_COLUMN_MEDIA_TYPE("media_type", "media_type"),
	PARAM_COLUMN_WEBSITE("website", "website_name"),
	PARAM_COLUMN_CHANNEL("channel", "channel_id"),
	PARAM_COLUMN_SENTIMENT("sentiment", "sentiment"), 
	PARAM_COLUMN_START_DATE("start_date", "rep_date"),
	PARAM_COLUMN_END_DATE("end_date", "rep_date"),
	PARAM_COLUMN_LIMIT("limit", "limit");

	private final String paramName;
	private final String columnName;
	
	private static Map<String, EnumTotalCount> mapper;
	
	private EnumTotalCount(String paramName, String columnName) {
		this.paramName = paramName;
		this.columnName = columnName;
	}

	public String getParamName() {
		return paramName;
	}

	public String getColumnName() {
		return columnName;
	}

	public static EnumTotalCount getEnum(String paramName){
        if(mapper == null){
        	mapper = new HashMap<String, EnumTotalCount>();
            for(EnumTotalCount obj : EnumTotalCount.values()){
            	mapper.put(obj.getParamName(), obj);
            }
        }
        return mapper.get(paramName);
    }

}
