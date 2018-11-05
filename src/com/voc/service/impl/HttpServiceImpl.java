package com.voc.service.impl;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;
import com.voc.service.HttpService;

public class HttpServiceImpl implements HttpService{
	private static final Logger LOGGER = LoggerFactory.getLogger(HttpServiceImpl.class);
	
//	private CloseableHttpClient httpClient = HttpClients.createDefault();
//	private CloseableHttpClient httpsClient = HttpClients.custom().setSSLHostnameVerifier(new NoopHostnameVerifier()).build();
	
	public <T> Object sendPost(boolean isSecurity, String api, Object data, Class<T> cls, boolean isArray){
		CloseableHttpClient http = null;
		try{
			HttpPost httpPost = new HttpPost(api);
			httpPost.addHeader("content-type", "application/json");
			if(data != null){
				StringEntity stringEntity = new StringEntity(new Gson().toJson(data), StandardCharsets.UTF_8);
				stringEntity.setContentType("application/json");
				httpPost.setEntity(stringEntity);
			}
			HttpResponse response = null;
			if(!isSecurity){
				http = HttpClients.createDefault();
			}else{
				TrustManager[] trustAllCerts = new TrustManager[1];
				TrustManager trustManager = new TrustManagerImpl(); 
				trustAllCerts[0] = trustManager;
				SSLContext sslContext = SSLContext.getInstance("SSL");
				sslContext.init(null, trustAllCerts, null);  
				http = HttpClients.custom().setSSLHostnameVerifier(new NoopHostnameVerifier(){
					 public boolean verify(String urlHostName, SSLSession session) {  
				            return true;  
					 }
				}).setSSLContext(sslContext).build();
			}
			response = http.execute(httpPost);
			Gson gson = getDateGson(cls,isArray);
			if(isArray){
				List<T> objList = gson.fromJson(EntityUtils.toString(response.getEntity()), new TypeToken<List<T>>(){}.getType());
				return objList;
			}else{
				T obj = gson.fromJson(EntityUtils.toString(response.getEntity()), cls);
				return obj;
			}
			
		}catch (Exception e) {
			return null;
		}finally {
			if(http != null){
				try {
					http.close();
				} catch (IOException e) {
					http = null;
				}
				
			}
		}
	}
	
	public int sendPost(boolean isSecurity, String api, Object data){
		CloseableHttpClient http = null;
		try{
			HttpPost httpPost = new HttpPost(api);
			httpPost.addHeader("content-type", "application/json");
			if(data != null){
				StringEntity stringEntity = new StringEntity(new Gson().toJson(data), StandardCharsets.UTF_8);
				stringEntity.setContentType("application/json");
				httpPost.setEntity(stringEntity);
			}
			HttpResponse response = null;
			if(!isSecurity){
				http = HttpClients.createDefault();
			}else{

				TrustManager[] trustAllCerts = new TrustManager[1];
				TrustManager trustManager = new TrustManagerImpl(); 
				trustAllCerts[0] = trustManager;
				SSLContext sslContext = SSLContext.getInstance("SSL");
				sslContext.init(null, trustAllCerts, null);  
				http = HttpClients.custom().setSSLHostnameVerifier(new NoopHostnameVerifier(){
					 public boolean verify(String urlHostName, SSLSession session) {  
				            return true;  
					 }
				}).setSSLContext(sslContext).build();
			}
			response = http.execute(httpPost);
			return response.getStatusLine().getStatusCode();
		}catch (Exception e) {
			return 401;
		}finally {
			if(http != null){
				try {
					http.close();
				} catch (IOException e) {
					http = null;
				}
				
			}
		}
	}
	
	public <T> Object sendGet(boolean isSecurity, String api, Class<T> cls, boolean isArray){
		CloseableHttpClient http = null;
		try{
			HttpGet httpGet = new HttpGet(api);
			HttpResponse response = null;
			if(!isSecurity){
				http = HttpClients.createDefault();
			}else{
				
				TrustManager[] trustAllCerts = new TrustManager[1];
				TrustManager trustManager = new TrustManagerImpl(); 
				trustAllCerts[0] = trustManager;
				SSLContext sslContext = SSLContext.getInstance("SSL");
				sslContext.init(null, trustAllCerts, null);  
				http = HttpClients.custom().setSSLHostnameVerifier(new NoopHostnameVerifier(){
					 public boolean verify(String urlHostName, SSLSession session) {  
				            return true;  
					 }
				}).setSSLContext(sslContext).build();
			}
			response = http.execute(httpGet);
			Gson gson = getDateGson(cls,isArray);
			if(isArray){
				List<T> objList = gson.fromJson(EntityUtils.toString(response.getEntity()), new TypeToken<List<T>>(){}.getType());
				return objList;
			}else{
				T obj = gson.fromJson(EntityUtils.toString(response.getEntity()), cls);
				return obj;
			}
		}catch (Exception e) {
			e.printStackTrace();
			return null;
		}finally {
			if(http != null){
				try {
					http.close();
				} catch (IOException e) {
					http = null;
				}
				
			}
		}
	}
	
	public int sendGet(boolean isSecurity, String api){
		CloseableHttpClient http = null;
		HttpResponse response = null;
		try{
			HttpGet httpGet = new HttpGet(api);
			
			if(!isSecurity){
				http = HttpClients.createDefault();
			}else{

				TrustManager[] trustAllCerts = new TrustManager[1];
				TrustManager trustManager = new TrustManagerImpl(); 
				trustAllCerts[0] = trustManager;
				SSLContext sslContext = SSLContext.getInstance("SSL");
				sslContext.init(null, trustAllCerts, null);  
				http = HttpClients.custom().setSSLHostnameVerifier(new NoopHostnameVerifier(){
					 public boolean verify(String urlHostName, SSLSession session) {  
				            return true;  
					 }
				}).setSSLContext(sslContext).build();
			}
			response = http.execute(httpGet);
			int statusCode = response.getStatusLine().getStatusCode();
			if (statusCode != HttpURLConnection.HTTP_OK) {
				LOGGER.error("response=" + response);
			}
			return statusCode;
		}catch (Exception e) {
			LOGGER.error(e.getMessage());
			return HttpURLConnection.HTTP_NOT_FOUND;
		}finally {
			if(http != null){
				try {
					http.close();
				} catch (IOException e) {
					http = null;
				}
				
			}
		}
	}
	
	public <T> Object sendPut(boolean isSecurity, String api, Object data, Class<T> cls, boolean isArray){
		CloseableHttpClient http = null;
		try{
			HttpPut httpPut = new HttpPut(api);
			httpPut.addHeader("content-type", "application/json");
			if(data != null){
				StringEntity stringEntity = new StringEntity(new Gson().toJson(data), StandardCharsets.UTF_8);
				stringEntity.setContentType("application/json");
				httpPut.setEntity(stringEntity);
			}
			HttpResponse response = null;
			if(!isSecurity){
				http = HttpClients.createDefault();
			}else{

				TrustManager[] trustAllCerts = new TrustManager[1];
				TrustManager trustManager = new TrustManagerImpl(); 
				trustAllCerts[0] = trustManager;
				SSLContext sslContext = SSLContext.getInstance("SSL");
				sslContext.init(null, trustAllCerts, null);  
				http = HttpClients.custom().setSSLHostnameVerifier(new NoopHostnameVerifier(){
					 public boolean verify(String urlHostName, SSLSession session) {  
				            return true;  
					 }
				}).setSSLContext(sslContext).build();
			}
			response = http.execute(httpPut);
			Gson gson = getDateGson(cls,isArray);
			if(isArray){
				List<T> objList = gson.fromJson(EntityUtils.toString(response.getEntity()), new TypeToken<List<T>>(){}.getType());
				return objList;
			}else{
				T obj = gson.fromJson(EntityUtils.toString(response.getEntity()), cls);
				return obj;
			}
			
		}catch (Exception e) {
			return null;
		}finally {
			if(http != null){
				try {
					http.close();
				} catch (IOException e) {
					http = null;
				}
				
			}
		}
	}
	
	public int sendPut(boolean isSecurity, String api, Object data){
		CloseableHttpClient http = null;
		try{
			HttpPut httpPut = new HttpPut(api);
			httpPut.addHeader("content-type", "application/json");
			if(data != null){
				StringEntity stringEntity = new StringEntity(new Gson().toJson(data), StandardCharsets.UTF_8);
				stringEntity.setContentType("application/json");
				httpPut.setEntity(stringEntity);
			}
			
			HttpResponse response = null;
			if(!isSecurity){
				http = HttpClients.createDefault();
			}else{

				TrustManager[] trustAllCerts = new TrustManager[1];
				TrustManager trustManager = new TrustManagerImpl(); 
				trustAllCerts[0] = trustManager;
				SSLContext sslContext = SSLContext.getInstance("SSL");
				sslContext.init(null, trustAllCerts, null);  
				http = HttpClients.custom().setSSLHostnameVerifier(new NoopHostnameVerifier(){
					 public boolean verify(String urlHostName, SSLSession session) {  
				            return true;  
					 }
				}).setSSLContext(sslContext).build();
			}
			response = http.execute(httpPut);
			return response.getStatusLine().getStatusCode();
		}catch (Exception e) {
			return 401;
		}finally {
			if(http != null){
				try {
					http.close();
				} catch (IOException e) {
					http = null;
				}
				
			}
		}
	}
	
	private<T> Gson getDateGson(final Class<T> cls,boolean isArray){
		try{
			GsonBuilder builder = new GsonBuilder();
			builder.registerTypeAdapter(Date.class, new JsonDeserializer<Date>() {
				public Date deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
					if(json.isJsonNull()){
						return null;
					}else{
						String dateStr = json.getAsJsonPrimitive().getAsString();
						if(StringUtils.isNotEmpty(dateStr)){
							return new Date(Long.valueOf(dateStr.trim()));
						}else{
							return null;
						}
					}
					
				}

			});
			
			if(isArray){
				builder.registerTypeAdapter(List.class, new JsonDeserializer<List<T>>() {
					public List<T> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
						if(json.isJsonNull()){
							return null;
						}else{
							JsonArray jsonArray = json.getAsJsonArray();
							List<T> dataList = new ArrayList<T>();
							Gson gson = getDateGson(cls, false);
							for (int i = 0; i < jsonArray.size(); i++) {
								dataList.add(gson.fromJson( jsonArray.get(i), cls));
								
							}
							return dataList;
						}
						
					}
	
				});
			}
			return builder.create();
		}catch (Exception e) {
			return new Gson();
		}
		
	}
	
}
