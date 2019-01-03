<%@ page language="java" contentType="application/json; charset=UTF-8"
	pageEncoding="UTF-8" session="false"%>
<%@ page trimDirectiveWhitespaces="true" %>
<%@ page import="org.json.JSONObject"%>
<%@ page import="com.voc.api.RootAPI" %>
<%@ page import="com.voc.api.topic.DeleteTopic" %>

<% 
	request.setCharacterEncoding("UTF-8");

	RootAPI deleteTopic = new DeleteTopic();
	String jsonStr = deleteTopic.processRequest(request);
	out.print(jsonStr);
%>
