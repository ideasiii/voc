package com.voc.service;

public interface HttpService {

	public <T> Object sendPost(boolean isSecurity, String api, Object data, Class<T> cls, boolean isArray);

	public int sendPost(boolean isSecurity, String api, Object data);

	public <T> Object sendGet(boolean isSecurity, String api, Class<T> cls, boolean isArray);
	
	public int sendGet(boolean isSecurity, String api);
	
	public <T> Object sendPut(boolean isSecurity, String api, Object data, Class<T> cls, boolean isArray);

	public int sendPut(boolean isSecurity, String api, Object data);
	
	
}
