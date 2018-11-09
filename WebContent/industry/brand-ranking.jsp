<%@ page language="java" contentType="application/json; charset=UTF-8"
	pageEncoding="UTF-8" session="false" trimDirectiveWhitespaces="true"%>
	
<%@ page import="org.json.JSONObject"%>
<%@ page import="com.voc.api.RootAPI" %>
<%@ page import="com.voc.api.industry.BrandRanking" %>
	
<% 
	request.setCharacterEncoding("UTF-8");
	
	BrandRanking brandRanking = new BrandRanking();
	String jsonStr = brandRanking.processRequest(request);
	out.print(jsonStr);
%>
	
	