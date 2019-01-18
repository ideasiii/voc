<%@ page language="java" contentType="application/json; charset=UTF-8"
	pageEncoding="UTF-8" session="false"%>
<%@ page trimDirectiveWhitespaces="true" %>
<%@ page import="com.voc.api.RootAPI" %>
<%@ page import="com.voc.api.topic.ArticleList" %>

<% 
	request.setCharacterEncoding("UTF-8");

	RootAPI articleList = new ArticleList();
	String jsonStr = articleList.processRequest(request);
	out.print(jsonStr);
%>
