package com.voc.filter;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import com.voc.common.ApiResponse;

public class ApiFilter implements Filter {
	private FilterConfig filterConfig = null;

	@Override
	public void init(FilterConfig filterConfig) throws ServletException {
		this.filterConfig = filterConfig;
	}

	@Override
	public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
			throws IOException, ServletException {
		
		servletResponse.setContentType("application/json");
		servletResponse.setCharacterEncoding("UTF-8");
		
//		String initParameter = this.filterConfig.getInitParameter("my-param");
//		System.out.println("debug: initParameter=" + initParameter);
//		Enumeration<String> parameterNames = servletRequest.getParameterNames();
//		while (parameterNames.hasMoreElements()) {
//			String name = parameterNames.nextElement();
//			String value = servletRequest.getParameter(name);
//			System.out.println("debug: name=" + name + ", value=" + value);
//		}
		
		// TODO: validate the token: 
//		PrintWriter out = servletResponse.getWriter();
//		String jsonStr = ApiResponse.error(ApiResponse.STATUS_UNAUTHORIZED).toString();
//		out.println(jsonStr);
		
		System.out.println("debug: ******* Before doFilter ******* ");
		filterChain.doFilter(servletRequest, servletResponse);
		System.out.println("debug: ******* After doFilter ******* ");
	}

	@Override
	public void destroy() {
		// Do nothing
	}

}
