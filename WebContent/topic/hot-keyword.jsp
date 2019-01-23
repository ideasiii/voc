<%@ page language="java" contentType="application/json; charset=UTF-8"
	pageEncoding="UTF-8" session="false"%>
<%@ page trimDirectiveWhitespaces="true" %>
<%@ page import="com.voc.api.RootAPI" %>
<%@ page import="com.voc.api.topic.HotKeyword" %>

<% 
	request.setCharacterEncoding("UTF-8");

	RootAPI hotKeyword = new HotKeyword();
	String jsonStr = hotKeyword.processRequest(request);
	out.print(jsonStr);
%>
