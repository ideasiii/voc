<%@ page language="java" contentType="application/json; charset=UTF-8"
	pageEncoding="UTF-8" session="false"%>
<%@ page trimDirectiveWhitespaces="true" %>
<%@ page import="org.json.JSONObject"%>
<%@ page import="com.voc.api.RootAPI" %>
<%@ page import="com.voc.api.industry.InfoModified" %>

<% 
	request.setCharacterEncoding("UTF-8");

	RootAPI infoModified = new InfoModified();
	String jsonStr = infoModified.processRequest(request);
	out.print(jsonStr);
%>
