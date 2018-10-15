package com.voc.api;

import javax.servlet.http.HttpServletRequest;

import org.json.JSONObject;

public abstract class RootAPI {

	public abstract JSONObject processRequest(HttpServletRequest request);

}
