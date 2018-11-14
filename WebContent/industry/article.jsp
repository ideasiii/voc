<%@ page language="java" contentType="application/json; charset=UTF-8"
	pageEncoding="UTF-8" session="false"%>
<%@ page trimDirectiveWhitespaces="true" %>
<%@ page import="com.voc.api.RootAPI" %>
<%@ page import="com.voc.api.industry.Article" %>

<% 
	request.setCharacterEncoding("UTF-8");

	RootAPI article = new Article();
	String jsonStr = article.processRequest(request);
	out.print(jsonStr);
%>
