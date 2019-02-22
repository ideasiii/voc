package com.voc.enums;

import java.util.HashMap;
import java.util.Map;

/**
 * sentiment(評價): 
 * - positive(偏正): 1
 * - neutral(中性): 0
 * - negative(偏負): -1
 * 
 */
public enum EnumSentiment {
	positive("1", "偏正"),
	neutral("0", "中性"),
	negative("-1", "偏負");

	private final String id;
	private final String name;
	
	private static Map<String, EnumSentiment> mapper;
	
	private EnumSentiment(String id, String name) {
		this.id = id;
		this.name = name;
	}

	public String getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public static EnumSentiment getEnum(String id){
        if(mapper == null){
        	mapper = new HashMap<String, EnumSentiment>();
            for(EnumSentiment obj : EnumSentiment.values()){
            	mapper.put(obj.getId(), obj);
            }
        }
        return mapper.get(id);
    }

}
