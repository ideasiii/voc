<%@ page language="java" contentType="application/json; charset=UTF-8"
	pageEncoding="UTF-8" session="false" trimDirectiveWhitespaces="true"%>
	
<%@ page import="org.json.JSONObject"%>
<%@ page import="com.voc.api.RootAPI" %>
<%@ page import="com.voc.api.industry.Comment" %>
	
<% 
	request.setCharacterEncoding("UTF-8");
	
	RootAPI comment = new Comment();
	String jsonStr = comment.processRequest(request);
	out.print(jsonStr);
%>
	
