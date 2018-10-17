<%@ page language="java" contentType="application/json; charset=UTF-8"
	pageEncoding="UTF-8" session="false" trimDirectiveWhitespaces="true"%>
	
<%@ page import="org.json.JSONObject"%>
<%@ page import="com.voc.api.industry.Trend" %>
	
<% 
	request.setCharacterEncoding("UTF-8");
	// JSONObject jobj = processRequest(request);
	// out.print(jobj.toString());
	
	Trend trend = new Trend();
	JSONObject jobj = trend.processRequest(request);
	out.print(jobj.toString());
%>
	
	