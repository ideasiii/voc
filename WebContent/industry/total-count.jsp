<%@ page language="java" contentType="application/json; charset=UTF-8"
	pageEncoding="UTF-8" session="false"%>
<%@ page trimDirectiveWhitespaces="true" %>
<%@ page import="org.json.JSONObject"%>
<%@ page import="com.voc.api.RootAPI" %>
<%@ page import="com.voc.api.industry.TotalCount" %>

<% 
	request.setCharacterEncoding("UTF-8");

	RootAPI totalCount = new TotalCount();
	String jsonStr = totalCount.processRequest(request);
	out.print(jsonStr);
%>
