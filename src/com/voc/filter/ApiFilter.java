package com.voc.filter;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.HttpURLConnection;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voc.api.RootAPI;
import com.voc.common.ApiResponse;
import com.voc.service.HttpService;
import com.voc.service.impl.HttpServiceImpl;

public class ApiFilter implements Filter {
	private static final Logger LOGGER = LoggerFactory.getLogger(ApiFilter.class);
	private static final HttpService HTTP_SERVICE = new HttpServiceImpl();
	private String api_url_token_validation; // https://ser.kong.srm.pw/dashboard/token/validation
	
	@Override
	public void init(FilterConfig filterConfig) throws ServletException {
		this.api_url_token_validation = filterConfig.getInitParameter("api_url_token_validation");
		LOGGER.info(" ******* ApiFilter init: api_url_token_validation=" + api_url_token_validation);
	}

	@Override
	public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
			throws IOException, ServletException {
		
		if (servletRequest instanceof HttpServletRequest) {
			String requestURL = ((HttpServletRequest) servletRequest).getRequestURL().toString();
			String queryString = ((HttpServletRequest) servletRequest).getQueryString();
			LOGGER.info("doFilter:==>" + requestURL + "?" + queryString);
		}
		
		// LOGGER.debug(" ******* Before doFilter ******* ");
		servletResponse.setContentType("application/json");
		servletResponse.setCharacterEncoding("UTF-8");
		
		// Validate the token:
		String token = servletRequest.getParameter(RootAPI.API_KEY);
		if (!this.checkToken(token)) {
			LOGGER.error("Invalide token=" + token);
			PrintWriter out = servletResponse.getWriter();
			String jsonStr = ApiResponse.unauthorizedError().toString();
			out.print(jsonStr);
		} else {
			filterChain.doFilter(servletRequest, servletResponse);
		}
		// LOGGER.debug(" ******* After doFilter ******* ");
	}

	@Override
	public void destroy() {
		// Do nothing
	}
	
	// -------------------------------------------------------------------
	
	private boolean checkToken(String token) {
		if (StringUtils.isBlank(token)) {
			return false;
		}
		String params = "?token=" + token;
		int statusCode = HTTP_SERVICE.sendGet(true, this.api_url_token_validation + params);
		return HttpURLConnection.HTTP_OK == statusCode;
	}

}
