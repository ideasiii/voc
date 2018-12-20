package com.voc.api.industry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
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
	
	private String strTableName;
	private String[] arrBrand;
	private String[] arrSeries;
	private String[] arrProduct;
	private String[] arrSource;
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
		
		JSONArray resArray = query();

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

		strIndustry = request.getParameter("industry");
		strBrand = request.getParameter("brand");
		strSeries = request.getParameter("series");
		strProduct = request.getParameter("product");
		strSource = request.getParameter("source");
		strWebsite = request.getParameter("website");
		strChannel = request.getParameter("channel");
		strFeatureGroup = request.getParameter("features");
		strSentiment = request.getParameter("sentiment");
		strStartDate = request.getParameter("start_date");
		strEndDate = request.getParameter("end_date");

		String strLimit = request.getParameter("limit");
		if (!StringUtils.isBlank(strLimit)) {
			try {
				this.nLimit = Integer.parseInt(strLimit);
			} catch (Exception e) {
				LOGGER.error(e.getMessage());
			}
		}

		strTableName = getTableName(paramMap);
		
		arrBrand = strBrand.split(PARAM_VALUES_SEPARATOR);
		arrSeries = strSeries.split(PARAM_VALUES_SEPARATOR);
		arrProduct = strProduct.split(PARAM_VALUES_SEPARATOR);
		arrSource = strSource.split(PARAM_VALUES_SEPARATOR);
		arrWebsite = strWebsite.split(PARAM_VALUES_SEPARATOR);
		arrChannel = strChannel.split(PARAM_VALUES_SEPARATOR);
		arrFeatureGroup = strFeatureGroup.split(PARAM_VALUES_SEPARATOR);
		arrSentiment = strSentiment.split(PARAM_VALUES_SEPARATOR);
	}
	
	private JSONObject validate() {
		if (StringUtils.isBlank(strBrand) || StringUtils.isBlank(strStartDate) || StringUtils.isBlank(strEndDate)) {
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
			sql.append("SELECT DISTINCT feature ");
			sql.append("FROM ").append(TABLE_INDUSTRY_FEATURE_KEYWORD_LIST).append(" ");
			sql.append("WHERE industry = ? ");
			sql.append("AND feature_group IN (");
			for (int i = 0; i < arrFeatureGroup.length; i++) {
				if (0 == i) sql.append("?");
				else sql.append(",?");
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
			LOGGER.debug("queryFeatureGroup SQL = " + pst.toString());
			
			rs = pst.executeQuery();
			while(rs.next()) {
				feature = rs.getString("feature");
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
			pst = conn.prepareStatement(genSelecttSQL());
			setWhereClauseValues(pst);

			String strPstSQL = pst.toString();
			LOGGER.info("strPstSQL: " + strPstSQL);

			List<JSONObject> brandList = new ArrayList<JSONObject>();
			Map<String, Integer> hash_brandMap = new HashMap<>();
			String brand;
			int count;

			rs = pst.executeQuery();
			while (rs.next()) {

				brand = rs.getString("brand");
				count = rs.getInt("count");

				JSONObject jobj = new JSONObject();
				jobj.put("brand", brand);
				jobj.put("count", count);
				brandList.add(jobj);
				hash_brandMap.put(brand, count);

				LOGGER.info("brandList: " + brandList);
			}

			for (int i = 0; i < arrBrand.length; i++) {
				String inputBrandItem = arrBrand[i];
				Integer outputCount = hash_brandMap.get(inputBrandItem);
				if (null == outputCount) {
					JSONObject jobj = new JSONObject();
					jobj.put("brand", inputBrandItem);
					jobj.put("count", 0);
				}
			}
			JSONArray out = new JSONArray(brandList);
			return out;

		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, pst, conn);
		}
		return null;
	}
	
	private String genSelecttSQL() {
		StringBuffer sql = new StringBuffer();
		sql.append("SELECT brand, SUM(reputation) AS count ");
		sql.append("FROM ").append(strTableName).append(" ");
		sql.append("WHERE brand IN (");

		for (int i = 0; i < arrBrand.length; i++) {
			if (0 == i) {
				sql.append(" ?");
			} else {
				sql.append(", ?");
			}
		}
		sql.append(") ");
		sql.append("AND DATE_FORMAT(date, '%Y-%m-%d') >= ? ");
		sql.append("AND DATE_FORMAT(date, '%Y-%m-%d') <= ? ");
		sql.append("GROUP BY brand ORDER BY count ");
		if (strSort.equalsIgnoreCase(Common.SORT_ASC)) {
			sql.append(Common.SORT_ASC).append(" ");
		} else if (strSort.equalsIgnoreCase(Common.SORT_DESC)) {
			sql.append(Common.SORT_DESC).append(" ");
		}
		sql.append("LIMIT ?");
		LOGGER.info("SQL : " + sql.toString());
		return sql.toString();
	}
	
	private void setWhereClauseValues(PreparedStatement pst) throws Exception {
		int idx = 0;

		for (String v : arrBrand) {
			int parameterIndex = idx + 1;
			pst.setObject(parameterIndex, v);
			// LOGGER.info("***" + parameterIndex + ":" + v);
			idx++;
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
