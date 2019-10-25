package com.voc.api.industry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
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
 *
 * Requirement Change: 1.參數:source修改為media_type(來源類型): 2.項目名稱 channel
 * 顯示由channel_name改為channel_display_name 3.前端改用POST method.
 * 
 * Requirement Change: 1.查詢featureList的industry改由industry_job_list
 * table內的ibk2_name來做查詢
 *
 * Requirement Change: 1.Output Result中的count改為feature_reputation中的文章數+相同搜尋條件feature_reputation_comment中的文章數
 * 
 */

public class HotFeature extends RootAPI {
	private static final Logger LOGGER = LoggerFactory.getLogger(HotFeature.class);
	private Map<String, String[]> paramMap;
	private String strIndustry;
	private String strBrand;
	private String strSeries;
	private String strProduct;
	private String strMediaType;
	private String strWebsite;
	private String strChannel;
	private String strFeatureGroup;
	private String strSentiment;
	private String strStartDate;
	private String strEndDate;
	private int nLimit = 10; // default
	private String strIbk2Name;

	private String[] arrBrand;
	private String[] arrSeries;
	private String[] arrProduct;
	private String[] arrMediaType;
	private String[] arrWebsite;
	private String[] arrChannel;
	private String[] arrFeatureGroup;
	private String[] arrSentiment;

	private List<String> featureList; // query from keyword list table
	private JSONArray sortedJsonArray;

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

		JSONObject featureJobj = new JSONObject();
		JSONObject commentJobj = new JSONObject();
		JSONObject jobj = new JSONObject();
		featureJobj = query(TABLE_FEATURE_REPUTATION);
		commentJobj = query(TABLE_FEATURE_REPUTATION_COMMENT);

		if (featureJobj.length() <= 0 && commentJobj.length() <= 0) {
			jobj = ApiResponse.successTemplate();
			jobj.put("result", "{}");
			return jobj.toString();
		}

		JSONArray resArray = new JSONArray();
		boolean querySuccess = mergeJSONObjects(featureJobj, commentJobj, resArray);

		if (querySuccess) {
			jobj = ApiResponse.successTemplate();
			jobj.put("result", this.sortedJsonArray);
			LOGGER.info("response: " + jobj.toString());
			return jobj.toString();
		}
		return ApiResponse.unknownError().toString();
	}

	private boolean hasRequiredParameters(Map<String, String[]> paramMap) {
		return paramMap.containsKey("industry") && paramMap.containsKey("start_date")
				&& paramMap.containsKey("end_date");
	}

	private void requestParams(HttpServletRequest request) {

		try {
			strIndustry = StringUtils.trimToEmpty(request.getParameter("industry"));
			strBrand = StringUtils.trimToEmpty(request.getParameter("brand"));
			strSeries = StringUtils.trimToEmpty(request.getParameter("series"));
			strProduct = StringUtils.trimToEmpty(request.getParameter("product"));
			strMediaType = StringUtils.trimToEmpty(request.getParameter("media_type"));
			strWebsite = StringUtils.trimToEmpty(request.getParameter("website"));
			strChannel = StringUtils.trimToEmpty(request.getParameter("channel"));
			strFeatureGroup = StringUtils.trimToEmpty(request.getParameter("features"));
			// strFeatureGroup = new String(strFeatureGroup.getBytes("ISO-8859-1"),"UTF-8");
			strSentiment = StringUtils.trimToEmpty(request.getParameter("sentiment"));
			strStartDate = StringUtils.trimToEmpty(request.getParameter("start_date"));
			strEndDate = StringUtils.trimToEmpty(request.getParameter("end_date"));
			strIbk2Name = getIndustryIbk2Name(this.strIndustry);

			String strLimit = request.getParameter("limit");
			if (!StringUtils.isBlank(strLimit)) {
				try {
					this.nLimit = Integer.parseInt(strLimit);
				} catch (Exception e) {
					LOGGER.error(e.getMessage());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		arrBrand = strBrand.split(PARAM_VALUES_SEPARATOR);
		arrSeries = strSeries.split(PARAM_VALUES_SEPARATOR);
		arrProduct = strProduct.split(PARAM_VALUES_SEPARATOR);
		arrMediaType = strMediaType.split(PARAM_VALUES_SEPARATOR);
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
				if (0 == i)
					sql.append(" ?");
				else
					sql.append(", ?");
			}
			sql.append(") ");

			conn = DBUtil.getConn();
			pst = conn.prepareStatement(sql.toString());
			int idx = 0;

			int industryIndex = idx + 1;
			pst.setObject(industryIndex, strIbk2Name);
			idx++;

			for (String fg : arrFeatureGroup) {
				int parameterIndex = idx + 1;
				pst.setObject(parameterIndex, fg);
				// LOGGER.info("***" + parameterIndex + ":" + fg);
				idx++;
			}
			LOGGER.debug("queryFeatureList SQL = " + pst.toString());

			rs = pst.executeQuery();
			while (rs.next()) {
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

	private JSONObject query(String table_name) {
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;

		try {
			int count;
			String feature;
			JSONObject jobj = new JSONObject();

			conn = DBUtil.getConn();
			pst = conn.prepareStatement(genSelectSQL(table_name));
			setWhereClauseValues(pst);

			String strPstSQL = pst.toString();
			LOGGER.info("strPstSQL: " + strPstSQL);

			rs = pst.executeQuery();
			while (rs.next()) {
				feature = rs.getString("features");
				count = rs.getInt("count");
				jobj.put(feature, count);
			}

			LOGGER.info("Out jobj: " + jobj);
			return jobj;

		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, pst, conn);
		}
		return null;
	}

	private String genSelectSQL(String table_name) {
		int nCount = 0;
		StringBuffer sql = new StringBuffer();
		sql.append("SELECT features, count(DISTINCT id) AS count ");
		sql.append("FROM ").append(table_name).append(" ");
		sql.append("WHERE ");

		if (!StringUtils.isBlank(strIndustry)) {
			if (0 < nCount) {
				sql.append("AND ");
			}
			sql.append("industry = ? ");
			nCount++;
		}

		if (!StringUtils.isBlank(strBrand)) {
			if (0 < nCount) {
				sql.append("AND ");
			}
			sql.append("brand IN (");
			for (int i = 0; i < arrBrand.length; i++) {
				if (0 == i)
					sql.append(" ?");
				else
					sql.append(", ?");
			}
			sql.append(") ");
			if (StringUtils.isEmpty(strSeries) && StringUtils.isEmpty(strProduct)) {
				sql.append(" AND series = '' AND product = '' ");
			}
			nCount++;
		}

		if (!StringUtils.isBlank(strSeries)) {
			if (0 < nCount) {
				sql.append("AND ");
			}
			sql.append("series IN (");
			for (int i = 0; i < arrSeries.length; i++) {
				if (0 == i)
					sql.append(" ?");
				else
					sql.append(", ?");
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
				if (0 == i)
					sql.append(" ?");
				else
					sql.append(", ?");
			}
			sql.append(") ");
			nCount++;
		}

		if (!StringUtils.isBlank(strMediaType)) {
			if (0 < nCount) {
				sql.append("AND ");
			}
			sql.append("media_type IN (");
			for (int i = 0; i < arrMediaType.length; i++) {
				if (0 == i)
					sql.append(" ?");
				else
					sql.append(", ?");
			}
			sql.append(") ");
			nCount++;
		}

		if (!StringUtils.isBlank(strWebsite)) {
			if (0 < nCount) {
				sql.append("AND ");
			}
			sql.append("website_name IN (");
			for (int i = 0; i < arrWebsite.length; i++) {
				if (0 == i)
					sql.append(" ?");
				else
					sql.append(", ?");
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
				if (0 == i)
					sql.append(" ?");
				else
					sql.append(", ?");
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
				if (0 == i)
					sql.append(" ?");
				else
					sql.append(", ?");
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
				if (0 == i)
					sql.append(" ?");
				else
					sql.append(", ?");
			}
			sql.append(") ");
			nCount++;
		}
		sql.append("AND DATE_FORMAT(rep_date, '%Y-%m-%d') >= ? ");
		sql.append("AND DATE_FORMAT(rep_date, '%Y-%m-%d') <= ? ");
		sql.append("GROUP BY features ORDER BY count DESC ");
		sql.append("LIMIT ?");

		// LOGGER.info("SQL : " + sql.toString());
		return sql.toString();
	}

	private void setWhereClauseValues(PreparedStatement pst) throws Exception {
		int idx = 0;

		if (!StringUtils.isBlank(strIndustry)) {
			int industryIndex = idx + 1;
			pst.setObject(industryIndex, strIndustry);
			idx++;
		}

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

		if (!StringUtils.isBlank(strMediaType)) {
			for (String v : arrMediaType) {
				int parameterIndex = idx + 1;
				pst.setObject(parameterIndex, v);
				// LOGGER.info("***" + parameterIndex + ":" + v);
				idx++;
			}
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

	private boolean mergeJSONObjects(JSONObject json1, JSONObject json2, JSONArray resArray) {
		JSONObject mergedJSON = new JSONObject();
		try {
			mergedJSON = new JSONObject(json1, JSONObject.getNames(json1));
			if (json2.length() > 0) {
				for (String featureKey : JSONObject.getNames(json2)) {
					if (mergedJSON.has(featureKey)) {
						mergedJSON.put(featureKey, (Integer) json2.get(featureKey) + (Integer) json1.get(featureKey));
					} else {
						mergedJSON.put(featureKey, json2.get(featureKey));
					}
				}
			}
			LOGGER.info("mergedJSON: " + mergedJSON);

			Iterator<String> iterKeys = mergedJSON.keys();
			while (iterKeys.hasNext()) {
				String key = iterKeys.next();
				JSONObject resJobj = new JSONObject();

				resJobj.put("feature", key);
				resJobj.put("count", mergedJSON.get(key));
				resArray.put(resJobj);
			}
			this.sortedJsonArray = this.getSortedResultArray(resArray, nLimit);
			return true;
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		}
		return false;
	}

	private JSONArray getSortedResultArray(JSONArray resultArray, int limit) {
		JSONArray sortedJsonArray = new JSONArray();

		// Step_1: parse your array in a list
		List<JSONObject> jsonList = new ArrayList<JSONObject>();
		for (int i = 0; i < resultArray.length(); i++) {
			jsonList.add(resultArray.getJSONObject(i));
		}

		// Step_2: then use collection.sort to sort the newly created list
		Collections.sort(jsonList, new Comparator<JSONObject>() {
			public int compare(JSONObject a, JSONObject b) {
				Integer valA = 0;
				Integer valB = 0;
				try {
					valA = (Integer) a.get("count");
					valB = (Integer) b.get("count");
				} catch (Exception e) {
					e.printStackTrace();
				}
				return valA.compareTo(valB) * -1;
			}
		});

		// Handle for limit:
		int listSize = jsonList.size();
		int recordSize = limit;
		if (limit > listSize) {
			recordSize = listSize;
		}
		jsonList = jsonList.subList(0, recordSize);

		// Step_3: Insert the sorted values in your array
		for (int i = 0; i < jsonList.size(); i++) {
			JSONObject jsonObject = jsonList.get(i);
			sortedJsonArray.put(jsonObject);
		}

		return sortedJsonArray;
	}

}
