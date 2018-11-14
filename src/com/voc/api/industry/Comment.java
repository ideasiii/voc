package com.voc.api.industry;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voc.api.RootAPI;
import com.voc.common.ApiResponse;
import com.voc.common.Common;

public class Comment extends RootAPI {
	private static final Logger LOGGER = LoggerFactory.getLogger(Comment.class);
	private Map<String, String[]> paramMap;
	private String strPostId;
	int nPageNum = 1;
	int nPageSize = 100;
	private final String strTableName = TABLE_COMMENT_LIST;
	
	@Override
	public String processRequest(HttpServletRequest request) {
		paramMap = request.getParameterMap();
		
		if (!hasRequiredParameters(paramMap)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER).toString();
		}
		requestParams(request);
		
		
		
		
		
		
		
		return null;
	}

	private boolean hasRequiredParameters(Map<String, String[]> paramMap) {
		return paramMap.containsKey("post_id");
	}
	
	private void requestParams(HttpServletRequest request) {
		
		strPostId = request.getParameter("post_id");
		
		String strPageNum = request.getParameter("page_num");
		if (!StringUtils.isBlank(strPageNum)) {
			try {
			this.nPageNum = Integer.parseInt(strPageNum);
			} catch (Exception e) {
				LOGGER.error(e.getMessage());
			}
		}
		
		String strPageSize = request.getParameter("page_size");
		if (!StringUtils.isBlank(strPageSize)) {
			try {
			this.nPageSize = Integer.parseInt(strPageSize);
			} catch (Exception e) {
				LOGGER.error(e.getMessage());
			}
		}
	}
	
	private JSONObject validate() {
		if (StringUtils.isBlank(strPostId)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}
		return null;
	}
	
	
	
	
}
