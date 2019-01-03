<%@ page language="java" contentType="application/json; charset=UTF-8"
	pageEncoding="UTF-8" session="false" trimDirectiveWhitespaces="true"%>
	
<%@ page import="org.json.JSONObject"%>
<%@ page import="com.voc.api.RootAPI" %>
<%@ page import="com.voc.api.topic.TopicState" %>
	
<% 
	request.setCharacterEncoding("UTF-8");
	
	RootAPI topicState = new TopicState();  
	String jsonStr = topicState.processRequest(request);
	out.print(jsonStr);
%>