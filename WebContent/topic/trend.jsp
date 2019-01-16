<%@ page language="java" contentType="application/json; charset=UTF-8"
	pageEncoding="UTF-8" session="false" trimDirectiveWhitespaces="true"%>
	
<%@ page import="org.json.JSONObject"%>
<%@ page import="com.voc.api.RootAPI" %>
<%@ page import="com.voc.api.topic.Trend" %>
	
<% 
	request.setCharacterEncoding("UTF-8");
	
	RootAPI trend = new Trend();
	String jsonStr = trend.processRequest(request);
	out.print(jsonStr);
%>
	
	