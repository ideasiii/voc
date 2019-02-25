package com.voc.api.industry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voc.api.RootAPI;
import com.voc.common.ApiResponse;
import com.voc.common.Common;
import com.voc.common.DBUtil;

/**
 * 
 * 
 * http://localhost:8080/voc/industry/hot-feature.jsp?industry=汽車產業&features=使用問題&website=Mobile01&start_date=2018-05-01&end_date=2018-05-15&limit=15
 * 
 * 新增sentiment:
 * http://localhost:8080/voc/industry/hot-feature.jsp?industry=輪胎&start_date=2019-02-09&end_date=2019-02-26&limit=15&sentiment=0
 * http://localhost:8080/voc/industry/hot-feature.jsp?industry=輪胎&features=種類&start_date=2019-02-09&end_date=2019-02-26&limit=15&sentiment=1
 */

public class HotFeature extends RootAPI {
	private static final Logger LOGGER = LoggerFactory.getLogger(HotFeature.class);
	private Map<String, String[]> paramMap;
	private String strIndustry;
	private String strBrand;
	private String strSeries;
	private String strProduct;
	private String strSource;
	private String strWebsite;
	private String strChannel;
	private String strFeatureGroup;
	private String strSentiment;
	private String strStartDate;
	private String strEndDate;
	private int nLimit = 10; // default
	
	private String[] arrBrand;
	private String[] arrSeries;
	private String[] arrProduct;
//	private String[] arrSource;
	private String[] arrWebsite;
	private String[] arrChannel;
	private String[] arrFeatureGroup;
	private String[] arrSentiment;
	
	private List<String> featureList; // query from keyword list table
	
	@Override
	public String processRequest(HttpServletRequest request) {
		paramMap = request.getParameterMap();
		
		if (!hasRequiredParameters(paramMap)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER).toString();
		}
		requestParams(request);
		JSONObject errorResponse = validate();
		if (null != errorResponse) {
			return errorResponse.toString();
		}
		if (paramMap.containsKey("features") && null != arrFeatureGroup) {
			featureList = queryFeatureList();
			LOGGER.info("***featureList: " + featureList);
		}
		
		JSONArray resArray = new JSONArray();
		if (StringUtils.isEmpty(this.strFeatureGroup) || (null != featureList && featureList.size() > 0)) {
			resArray = query();
		}
	
		if (null != resArray) {
			JSONObject jobj = ApiResponse.successTemplate();
			jobj.put("result", resArray);
			LOGGER.info("response: " + jobj.toString());
			return jobj.toString();
		}
		return ApiResponse.unknownError().toString();
	}

	private boolean hasRequiredParameters(Map<String, String[]> paramMap) {
		return paramMap.containsKey("industry") && paramMap.containsKey("start_date") && paramMap.containsKey("end_date");
	}
	
	private void requestParams(HttpServletRequest request) {

		strIndustry = StringUtils.trimToEmpty(request.getParameter("industry"));
		strBrand = StringUtils.trimToEmpty(request.getParameter("brand"));
		strSeries = StringUtils.trimToEmpty(request.getParameter("series"));
		strProduct = StringUtils.trimToEmpty(request.getParameter("product"));
		strSource = StringUtils.trimToEmpty(request.getParameter("source"));
		strWebsite = StringUtils.trimToEmpty(request.getParameter("website"));
		strChannel = StringUtils.trimToEmpty(request.getParameter("channel"));
		strFeatureGroup = StringUtils.trimToEmpty(request.getParameter("features"));
		strSentiment = StringUtils.trimToEmpty(request.getParameter("sentiment"));
		strStartDate = StringUtils.trimToEmpty(request.getParameter("start_date"));
		strEndDate = StringUtils.trimToEmpty(request.getParameter("end_date"));

		String strLimit = request.getParameter("limit");
		if (!StringUtils.isBlank(strLimit)) {
			try {
				this.nLimit = Integer.parseInt(strLimit);
			} catch (Exception e) {
				LOGGER.error(e.getMessage());
			}
		}
		
		arrBrand = strBrand.split(PARAM_VALUES_SEPARATOR);
		arrSeries = strSeries.split(PARAM_VALUES_SEPARATOR);
		arrProduct = strProduct.split(PARAM_VALUES_SEPARATOR);
	//	arrSource = strSource.split(PARAM_VALUES_SEPARATOR);
		arrWebsite = strWebsite.split(PARAM_VALUES_SEPARATOR);
		arrChannel = strChannel.split(PARAM_VALUES_SEPARATOR);
		arrFeatureGroup = strFeatureGroup.split(PARAM_VALUES_SEPARATOR);
		arrSentiment = strSentiment.split(PARAM_VALUES_SEPARATOR);
	}
	
	private JSONObject validate() {
		if (StringUtils.isBlank(strIndustry) || StringUtils.isBlank(strStartDate) || StringUtils.isBlank(strEndDate)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}

		if (!Common.isValidDate(strStartDate, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid start_date.");
		}
		strStartDate = Common.formatDate(strStartDate, "yyyy-MM-dd");

		if (!Common.isValidDate(strEndDate, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid end_date.");
		}
		strEndDate = Common.formatDate(strEndDate, "yyyy-MM-dd");

		if (!Common.isValidStartDate(strStartDate, strEndDate, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid period values.");
		}
		return null;
	}
	
	private List<String> queryFeatureList() {
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		String feature;
		List<String> featureList = new ArrayList<>();
		
		try {
			StringBuffer sql = new StringBuffer();
			sql.append("SELECT DISTINCT feature AS f ");
			sql.append("FROM ").append(TABLE_INDUSTRY_FEATURE_KEYWORD_LIST).append(" ");
			sql.append("WHERE industry = ? ");
			sql.append("AND feature_group IN (");
			for (int i = 0; i < arrFeatureGroup.length; i++) {
				if (0 == i) sql.append(" ?");
				else sql.append(", ?");
			}
			sql.append(") ");
			
			conn = DBUtil.getConn();
			pst = conn.prepareStatement(sql.toString());
			int idx = 0;
			
			int industryIndex = idx + 1;
			pst.setObject(industryIndex, strIndustry);
			idx++;
			
			for (String fg : arrFeatureGroup) {
				int parameterIndex = idx + 1;
				pst.setObject(parameterIndex, fg);
				LOGGER.info("***" + parameterIndex + ":" + fg);
				idx++;
			}
			LOGGER.debug("queryFeatureList SQL = " + pst.toString());
			
			rs = pst.executeQuery();
			while(rs.next()) {
				feature = rs.getString("f");
				featureList.add(feature);
			}
 			return featureList;
			
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, pst, conn);
		}
		return null;
	}
	
	private JSONArray query() {
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;

		try {
			conn = DBUtil.getConn();
			pst = conn.prepareStatement(genSelectSQL());
			setWhereClauseValues(pst);
			
			String strPstSQL = pst.toString();
			LOGGER.info("strPstSQL: " + strPstSQL);
			//////////
			int count;
			String feature;
			JSONArray out = new JSONArray();
			rs = pst.executeQuery();
			while (rs.next()) {
				feature = rs.getString("features");
				count = rs.getInt("count");
				
				JSONObject jobj = new JSONObject();
				jobj.put("feature", feature);
				jobj.put("count", count);
				out.put(jobj);
			}
			LOGGER.info("Out array: " + out);
			return out;

		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, pst, conn);
		}
		return null;
	}
	
	private String genSelectSQL() {
		int nCount = 0;
		StringBuffer sql = new StringBuffer();
		sql.append("SELECT features, count(DISTINCT id) AS count ");
		sql.append("FROM ").append(TABLE_FEATURE_REPUTATION).append(" ");
		sql.append("WHERE ");
		
		if (!StringUtils.isBlank(strBrand)) {
			if (0 < nCount) {
				sql.append("AND ");
			}
		sql.append("brand IN (");
		for (int i = 0; i < arrBrand.length; i++) {
			if (0 == i) sql.append(" ?");
			else sql.append(", ?");
		}
		sql.append(") ");
		nCount++;
		}
		
		if (!StringUtils.isBlank(strSeries)) {
			if (0 < nCount) {
				sql.append("AND ");
			}
		sql.append("series IN (");
		for (int i = 0; i < arrSeries.length; i++) {
			if (0 == i) sql.append(" ?");
			else sql.append(", ?");
		}
		sql.append(") ");
		nCount++;
		}
		
		if (!StringUtils.isBlank(strProduct)) {
			if (0 < nCount) {
				sql.append("AND ");
			}
		sql.append("product IN (");
		for (int i = 0; i < arrProduct.length; i++) {
			if (0 == i) sql.append(" ?");
			else sql.append(", ?");
		}
		sql.append(") ");
		nCount++;
		}

		if (!StringUtils.isBlank(strSource)) {
			// 尚未啟用
		}
		
		if (!StringUtils.isBlank(strWebsite)) {
			if (0 < nCount) {
				sql.append("AND ");
			}
			sql.append("website_name IN (");
			for (int i = 0; i < arrWebsite.length; i++) {
				if (0 == i) sql.append(" ?");
				else sql.append(", ?");
			}
			sql.append(") ");
			nCount++;
			}
		
		if (!StringUtils.isBlank(strChannel)) {
			if (0 < nCount) {
				sql.append("AND ");
			}
		sql.append("channel_id IN (");
		for (int i = 0; i < arrChannel.length; i++) {
			if (0 == i) sql.append(" ?");
			else sql.append(", ?");
		}
		sql.append(") ");
		nCount++;
		}
		
		if (!StringUtils.isBlank(strFeatureGroup) && 0 < featureList.size() && null != featureList) {
			if (0 < nCount) {
				sql.append("AND ");
			}
			sql.append("features IN (");
			for (int i = 0; i < featureList.size(); i++) {
				if (0 == i) sql.append(" ?");
				else sql.append(", ?");
			}
			sql.append(") ");
			nCount++;
			}
		
		if (!StringUtils.isBlank(strSentiment)) {
			if (0 < nCount) {
				sql.append("AND ");
			}
		sql.append("sentiment IN (");
		for (int i = 0; i < arrSentiment.length; i++) {
			if (0 == i) sql.append(" ?");
			else sql.append(", ?");
		}
		sql.append(") ");
		nCount++;
		}
		sql.append("AND DATE_FORMAT(date, '%Y-%m-%d') >= ? ");
		sql.append("AND DATE_FORMAT(date, '%Y-%m-%d') <= ? ");
		sql.append("GROUP BY features ORDER BY count DESC ");
		sql.append("LIMIT ?");
		
		LOGGER.info("SQL : " + sql.toString());
		return sql.toString();
	}
	
	private void setWhereClauseValues(PreparedStatement pst) throws Exception {
		int idx = 0;

		if (!StringUtils.isBlank(strBrand)) {
		for (String v : arrBrand) {
			int parameterIndex = idx + 1;
			pst.setObject(parameterIndex, v);
			// LOGGER.info("***" + parameterIndex + ":" + v);
			idx++;
			}
		}
		
		if (!StringUtils.isBlank(strSeries)) {
			for (String v : arrSeries) {
				int parameterIndex = idx + 1;
				pst.setObject(parameterIndex, v);
				// LOGGER.info("***" + parameterIndex + ":" + v);
				idx++;
				}
			}
		
		if (!StringUtils.isBlank(strProduct)) {
			for (String v : arrProduct) {
				int parameterIndex = idx + 1;
				pst.setObject(parameterIndex, v);
				// LOGGER.info("***" + parameterIndex + ":" + v);
				idx++;
				}
			}
		
		if (!StringUtils.isBlank(strSource)) {
			// 尚未啟用
		}
		
		if (!StringUtils.isBlank(strWebsite)) {
			for (String v : arrWebsite) {
				int parameterIndex = idx + 1;
				pst.setObject(parameterIndex, v);
				// LOGGER.info("***" + parameterIndex + ":" + v);
				idx++;
				}
			}
		
		if (!StringUtils.isBlank(strChannel)) {
			for (String v : arrChannel) {
				int parameterIndex = idx + 1;
				pst.setObject(parameterIndex, v);
				// LOGGER.info("***" + parameterIndex + ":" + v);
				idx++;
				}
			}
		
		if (!StringUtils.isBlank(strFeatureGroup)) {
			for (String v : featureList) {
				int parameterIndex = idx + 1;
				pst.setObject(parameterIndex, v);
				LOGGER.info("***" + parameterIndex + ":" + v);
				idx++;
				}
			}
		
		if (!StringUtils.isBlank(strSentiment)) {
			for (String v : arrSentiment) {
				int parameterIndex = idx + 1;
				pst.setObject(parameterIndex, v);
				// LOGGER.info("***" + parameterIndex + ":" + v);
				idx++;
				}
		}
		
		int startDateIndex = idx + 1;
		pst.setObject(startDateIndex, strStartDate);
		idx++;

		int endDateIndex = idx + 1;
		pst.setObject(endDateIndex, strEndDate);
		idx++;

		int limitIndex = idx + 1;
		pst.setObject(limitIndex, nLimit);
		idx++;
	}
	
	
}
