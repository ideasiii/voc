<%@ page language="java" contentType="application/json; charset=UTF-8"
	pageEncoding="UTF-8" session="false"%>
<%@ page trimDirectiveWhitespaces="true" %>
<%@ page import="com.voc.api.RootAPI" %>
<%@ page import="com.voc.api.topic.KeywordArticleList" %>

<% 
	request.setCharacterEncoding("UTF-8");

	RootAPI keywordArticleList = new KeywordArticleList();
	String jsonStr = keywordArticleList.processRequest(request);
	out.print(jsonStr);
%>
