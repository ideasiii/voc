<%@ page language="java" contentType="application/json; charset=UTF-8"
	pageEncoding="UTF-8" session="false"%>
<%@ page import="org.json.JSONObject"%>
	
	
<% 
	request.setCharacterEncoding("UTF-8");
	JSONObject jobj = processRequest(request);
	out.print(jobj.toString());
%>