<%@ page language="java" contentType="application/json; charset=UTF-8"
	pageEncoding="UTF-8" session="false" trimDirectiveWhitespaces="true"%>
	
<%@ page import="org.json.JSONObject"%>
<%@ page import="com.voc.api.RootAPI" %>
<%@ page import="com.voc.api.industry.HotFeature" %>
	
<%
		request.setCharacterEncoding("UTF-8");
		
		RootAPI hotFeature = new HotFeature();  
		String jsonStr = hotFeature.processRequest(request);
		out.print(jsonStr);
	%>
	
