package com.voc.service;

import java.net.HttpURLConnection;

import com.voc.service.impl.HttpServiceImpl;

/**
 * Only for Test: 
 *
 */
public class HttpServiceTest {
	private static final HttpService HTTP_SERVICE = new HttpServiceImpl();
	private static final String API_URL_TOKEN_VALIDATION = "https://ser.kong.srm.pw/dashboard/token/validation";
	
	private boolean checkToken(String token) {
		String params = "?token=" + token;
		int statusCode = HTTP_SERVICE.sendGet(true, API_URL_TOKEN_VALIDATION + params);
		return HttpURLConnection.HTTP_OK == statusCode;
	}

	// **********************************************************
	
	// For Test: 
	public static void main(String[] args) {
		String token = "ssf6tc0spjsca8f2797rlfmaq";
		
		HttpServiceTest httpServiceTest = new HttpServiceTest();
		if (httpServiceTest.checkToken(token)) {
			System.out.println("The token is valiad.");
		} else {
			System.out.println("The token is invaliad!!!");
		}
	}

}
