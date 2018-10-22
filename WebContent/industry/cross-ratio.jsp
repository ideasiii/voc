<%@ page language="java" contentType="application/json; charset=UTF-8"
	pageEncoding="UTF-8" session="false"%>
<%@ page trimDirectiveWhitespaces="true" %>
<%@ page import="org.json.JSONObject"%>
<%@ page import="com.voc.api.RootAPI" %>
<%@ page import="com.voc.api.industry.CrossRatio" %>

<% 
	request.setCharacterEncoding("UTF-8");

	RootAPI crossRatio = new CrossRatio();
	JSONObject jobj = crossRatio.processRequest(request);
	out.print(jobj.toString());
%>
