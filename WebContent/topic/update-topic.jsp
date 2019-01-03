<%@ page language="java" contentType="application/json; charset=UTF-8"
	pageEncoding="UTF-8" session="false"%>
<%@ page trimDirectiveWhitespaces="true" %>
<%@ page import="org.json.JSONObject"%>
<%@ page import="com.voc.api.RootAPI" %>
<%@ page import="com.voc.api.topic.UpdateTopic" %>

<% 
	request.setCharacterEncoding("UTF-8");

	RootAPI updateTopic = new UpdateTopic();
	String jsonStr = updateTopic.processRequest(request);
	out.print(jsonStr);
%>
