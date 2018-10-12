package com.voc.api;

import javax.servlet.http.HttpServletRequest;

import org.json.JSONObject;

public interface RootAPI {

	public JSONObject processRequest(HttpServletRequest request);

}
