<%@ page language="java" contentType="application/json; charset=UTF-8"
	pageEncoding="UTF-8" session="false"%>
<%@ page trimDirectiveWhitespaces="true" %>
<%@ page import="org.json.JSONObject"%>
<%@ page import="com.voc.api.RootAPI" %>
<%@ page import="com.voc.api.industry.TotalCount" %>

<% 
	request.setCharacterEncoding("UTF-8");

	RootAPI totalCount = new TotalCount();
	JSONObject jobj = totalCount.processRequest(request);
	out.print(jobj.toString());
%>
