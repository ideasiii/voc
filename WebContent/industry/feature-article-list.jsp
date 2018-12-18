<%@ page language="java" contentType="application/json; charset=UTF-8"
	pageEncoding="UTF-8" session="false"%>
<%@ page trimDirectiveWhitespaces="true" %>
<%@ page import="com.voc.api.RootAPI" %>
<%@ page import="com.voc.api.industry.FeatureArticleList" %>

<% 
	request.setCharacterEncoding("UTF-8");

	RootAPI featureArticleList = new FeatureArticleList();
	String jsonStr = featureArticleList.processRequest(request);
	out.print(jsonStr);
%>
