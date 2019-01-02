<%@ page language="java" contentType="application/json; charset=UTF-8"
	pageEncoding="UTF-8" session="false"%>
<%@ page trimDirectiveWhitespaces="true" %>
<%@ page import="org.json.JSONObject"%>
<%@ page import="com.voc.api.RootAPI" %>
<%@ page import="com.voc.api.topic.AddTopic" %>

<% 
	request.setCharacterEncoding("UTF-8");

	RootAPI addTopic = new AddTopic();
	String jsonStr = addTopic.processRequest(request);
	out.print(jsonStr);
%>
