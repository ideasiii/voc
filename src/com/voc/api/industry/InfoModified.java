package com.voc.api.industry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voc.api.RootAPI;
import com.voc.api.industry.InfoModified;
import com.voc.common.ApiResponse;
import com.voc.common.DBUtil;

public class InfoModified extends RootAPI {
	private static final Logger LOGGER = LoggerFactory.getLogger(InfoModified.class);
	private String industry;
	private String brand;
	private String type;
	private String name_new;
	private String name_old;

	@Override
	public String processRequest(HttpServletRequest request) {
		JSONObject initErrorResponse = this.requestAndInitValidateParams(request);
		if (initErrorResponse != null) {
			LOGGER.info(initErrorResponse.toString());
			return initErrorResponse.toString();
		}
		int recordCnt = this.selectNewName();
		if (recordCnt == 0) {
			String errorMsg = ApiResponse
					.error(ApiResponse.STATUS_DATA_NOT_FOUND, "Duplicate Name Exists!")
					.toString();
			LOGGER.info(errorMsg);
			return errorMsg;
		}
	
		
		
		
		return ApiResponse.unknownError().toString();
	}

	private JSONObject requestAndInitValidateParams(HttpServletRequest request) {

		this.industry = StringUtils.trimToEmpty(request.getParameter("industry"));
		this.brand = StringUtils.trimToEmpty(request.getParameter("brand"));
		this.type = StringUtils.trimToEmpty(request.getParameter("type"));
		this.name_new = StringUtils.trimToEmpty(request.getParameter("name_new"));
		this.name_old = StringUtils.trimToEmpty(request.getParameter("name_old"));

		if (StringUtils.isBlank(this.industry) && StringUtils.isBlank(this.brand) && StringUtils.isBlank(this.type)
				&& StringUtils.isBlank(this.name_new) && StringUtils.isBlank(this.name_old)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}
		
		if (!this.type.equals("brand") || !this.type.equals("product")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid Type.");
		}
		return null;
	}

	private int checkBrandNotExist(String table_name) {
		int recordCnt = 0;
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		String sql = "SELECT count(1) as count FROM " + table_name + " WHERE industry = ? AND brand = ?";
		
		try {
			conn = DBUtil.getConn();
			pst = conn.prepareStatement(sql);
			pst.setObject(1, this.industry);
			pst.setObject(2, this.brand);
			rs = pst.executeQuery();
			
			
			
			
			
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, pst, conn);
		}
		return recordCnt;
	}
	
	
	
	

}
