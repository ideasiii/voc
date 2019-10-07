package com.voc.api.industry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Map;

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
	private static final String TYPE_BRAND = "brand";
	private static final String TYPE_PRODUCT = "product";
	private static final String TYPE_SERIES = "series";
	private String industry;
	private String brand;
	private String type;
	private String name_new;
	private String name_old;
//	private String table_br = "brand_reputation_dev";
//	private String table_pr = "product_reputation_dev";
//	private String table_fr = "feature_reputation_dev";

	@Override
	public String processRequest(HttpServletRequest request) {
		JSONObject initErrorResponse = this.requestAndInitValidateParams(request);
		if (initErrorResponse != null) {
			LOGGER.info(initErrorResponse.toString());
			return initErrorResponse.toString();
		}
		boolean isSuccess = false;

		switch (this.type) {
		case TYPE_BRAND:
			int recordCntBrand = this.checkBrandNotExist(TABLE_BRAND_REPUTATION) + this.checkBrandNotExist(TABLE_PRODUCT_REPUTATION)
					+ this.checkBrandNotExist(TABLE_FEATURE_REPUTATION) + this.checkBrandNotExist(TABLE_BRAND_REPUTATION_COMMENT)
					+ this.checkBrandNotExist(TABLE_PRODUCT_REPUTATION_COMMENT)
					+ this.checkBrandNotExist(TABLE_FEATURE_REPUTATION_COMMENT);
			if (recordCntBrand != 0) {
				String errorMsg = ApiResponse.error(ApiResponse.STATUS_DATA_NOT_FOUND, "Duplicate Name Exists!")
						.toString();
				LOGGER.info(errorMsg);
				return errorMsg;
			}
			isSuccess = this.updateBrandName(TABLE_BRAND_REPUTATION) && this.updateBrandName(TABLE_PRODUCT_REPUTATION)
					&& this.updateBrandName(TABLE_FEATURE_REPUTATION) && this.updateBrandName(TABLE_BRAND_REPUTATION_COMMENT)
					&& this.updateBrandName(TABLE_PRODUCT_REPUTATION_COMMENT)
					&& this.updateBrandName(TABLE_FEATURE_REPUTATION_COMMENT);
			if (isSuccess) {
				boolean insertLog = insertChangeLog(this.industry, this.brand, this.name_new, this.name_old, this.type);
				if (insertLog) {
					return ApiResponse.successTemplate().toString();
				}
			}
		case TYPE_PRODUCT:
			int recordCntProduct = this.checkProductNotExist(TABLE_PRODUCT_REPUTATION) + this.checkProductNotExist(TABLE_FEATURE_REPUTATION)
					+ this.checkProductNotExist(TABLE_PRODUCT_REPUTATION_COMMENT)
					+ this.checkProductNotExist(TABLE_FEATURE_REPUTATION_COMMENT);
			if (recordCntProduct != 0) {
				String errorMsg = ApiResponse.error(ApiResponse.STATUS_DATA_NOT_FOUND, "Duplicate Name Exists!")
						.toString();
				LOGGER.info(errorMsg);
				return errorMsg;
			}
			isSuccess = this.updateProductName(TABLE_PRODUCT_REPUTATION) && this.updateProductName(TABLE_FEATURE_REPUTATION)
					&& this.updateProductName(TABLE_PRODUCT_REPUTATION_COMMENT)
					&& this.updateProductName(TABLE_FEATURE_REPUTATION_COMMENT);
			if (isSuccess) {
				boolean insertLog = insertChangeLog(this.industry, this.brand, this.name_new, this.name_old, this.type);
				if (insertLog) {
					return ApiResponse.successTemplate().toString();
				}
			}
		case TYPE_SERIES:
			int recordCntSeries = this.checkSeriesNotExist(TABLE_PRODUCT_REPUTATION) + this.checkSeriesNotExist(TABLE_FEATURE_REPUTATION)
					+ +this.checkSeriesNotExist(TABLE_PRODUCT_REPUTATION_COMMENT)
					+ +this.checkSeriesNotExist(TABLE_FEATURE_REPUTATION_COMMENT);
			if (recordCntSeries != 0) {
				String errorMsg = ApiResponse.error(ApiResponse.STATUS_DATA_NOT_FOUND, "Duplicate Name Exists!")
						.toString();
				LOGGER.info(errorMsg);
				return errorMsg;
			}
			isSuccess = this.updateSeriesName(TABLE_PRODUCT_REPUTATION) && this.updateSeriesName(TABLE_FEATURE_REPUTATION)
					&& this.updateSeriesName(TABLE_PRODUCT_REPUTATION_COMMENT)
					&& this.updateSeriesName(TABLE_FEATURE_REPUTATION_COMMENT);
			if (isSuccess) {
				boolean insertLog = insertChangeLog(this.industry, this.brand, this.name_new, this.name_old, this.type);
				if (insertLog) {
					return ApiResponse.successTemplate().toString();
				}
			}
		default:
			return ApiResponse.unknownError().toString();
		}
	}

	private JSONObject requestAndInitValidateParams(HttpServletRequest request) {

		if (!hasRequiredParameters(request)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}
		this.industry = StringUtils.trimToEmpty(request.getParameter("industry"));
		this.brand = StringUtils.trimToEmpty(request.getParameter("brand"));
		this.type = StringUtils.trimToEmpty(request.getParameter("type"));
		this.name_new = StringUtils.trimToEmpty(request.getParameter("name_new"));
		this.name_old = StringUtils.trimToEmpty(request.getParameter("name_old"));
		LOGGER.info(type);

		if (StringUtils.isBlank(this.industry) || StringUtils.isBlank(this.brand) || StringUtils.isBlank(this.type)
				|| StringUtils.isBlank(this.name_new) || StringUtils.isBlank(this.name_old)) {
			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}
		if (!checkValidType(this.type)) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid Type.");
		}
		return null;
	}

	public boolean hasRequiredParameters(final HttpServletRequest request) {
		Map paramMap = request.getParameterMap();
		return paramMap.containsKey("industry") && paramMap.containsKey("brand") && paramMap.containsKey("type")
				&& paramMap.containsKey("name_new") && paramMap.containsKey("name_old");
	}

	public boolean checkValidType(final String t) {
		return t.equals(TYPE_BRAND) || t.equals(TYPE_PRODUCT) || t.equals(TYPE_SERIES);
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
			pst.setObject(2, this.name_new);
			rs = pst.executeQuery();
			while (rs.next()) {
				recordCnt = rs.getInt("count");
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, pst, conn);
		}
		return recordCnt;
	}

	private int checkProductNotExist(String table_name) {
		int recordCnt = 0;
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		String sql = "SELECT count(1) as count FROM " + table_name
				+ " WHERE industry = ? AND brand = ? AND product = ?";

		try {
			conn = DBUtil.getConn();
			pst = conn.prepareStatement(sql);
			pst.setObject(1, this.industry);
			pst.setObject(2, this.brand);
			pst.setObject(3, this.name_new);
			rs = pst.executeQuery();
			while (rs.next()) {
				recordCnt = rs.getInt("count");
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, pst, conn);
		}
		return recordCnt;
	}

	private int checkSeriesNotExist(String table_name) {
		int recordCnt = 0;
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		String sql = "SELECT count(1) as count FROM " + table_name + " WHERE industry = ? AND brand = ? AND series = ?";

		try {
			conn = DBUtil.getConn();
			pst = conn.prepareStatement(sql);
			pst.setObject(1, this.industry);
			pst.setObject(2, this.brand);
			pst.setObject(3, this.name_new);
			rs = pst.executeQuery();
			while (rs.next()) {
				recordCnt = rs.getInt("count");
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, pst, conn);
		}
		return recordCnt;
	}

	private boolean updateBrandName(String table_name) {
		Connection conn = null;
		PreparedStatement pst = null;
		String sql = "UPDATE " + table_name + " SET brand=? WHERE industry = ? AND brand = ?";

		try {
			conn = DBUtil.getConn();
			pst = conn.prepareStatement(sql);
			pst.setObject(1, this.name_new);
			pst.setObject(2, this.industry);
			pst.setObject(3, this.brand);
			pst.execute();
			LOGGER.info("Table: " + table_name + " UPDATE OK!!!");
			return true;

		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.closePreparedStatement(pst);
			DBUtil.closeConn(conn);
		}
		return false;
	}

	private boolean updateProductName(String table_name) {
		Connection conn = null;
		PreparedStatement pst = null;
		String sql = "UPDATE " + table_name + " SET product=? WHERE industry = ? AND brand = ? AND product = ?";

		try {
			conn = DBUtil.getConn();
			pst = conn.prepareStatement(sql);
			pst.setObject(1, this.name_new);
			pst.setObject(2, this.industry);
			pst.setObject(3, this.brand);
			pst.setObject(4, this.name_old);
			pst.execute();
			LOGGER.info("Table: " + table_name + " UPDATE OK!!!");
			return true;

		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.closePreparedStatement(pst);
			DBUtil.closeConn(conn);
		}
		return false;
	}

	private boolean updateSeriesName(String table_name) {
		Connection conn = null;
		PreparedStatement pst = null;
		String sql = "UPDATE " + table_name + " SET series=? WHERE industry = ? AND brand = ? AND series = ?";

		try {
			conn = DBUtil.getConn();
			pst = conn.prepareStatement(sql);
			pst.setObject(1, this.name_new);
			pst.setObject(2, this.industry);
			pst.setObject(3, this.brand);
			pst.setObject(4, this.name_old);
			pst.execute();
			LOGGER.info("Table: " + table_name + " UPDATE OK!!!");
			return true;

		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.closePreparedStatement(pst);
			DBUtil.closeConn(conn);
		}
		return false;
	}

	private boolean insertChangeLog(String strIndustry, String strBrand, String strNameNew, String strNameOld,
			String strType) {
		Connection conn = null;
		PreparedStatement pst = null;
		Timestamp ts = new Timestamp(System.currentTimeMillis());
		String sql = "INSERT INTO " + TABLE_INDUSTRY_NAME_CHANGE_LOG
				+ " (industry, brand, name_new, name_old, type, update_time) values (?, ?, ?, ?, ?, ?)";

		try {
			conn = DBUtil.getConn();
			pst = conn.prepareStatement(sql);
			pst.setObject(1, strIndustry);
			pst.setObject(2, strBrand);
			pst.setObject(3, strNameNew);
			pst.setObject(4, strNameOld);
			pst.setObject(5, strType);
			pst.setObject(6, ts);
			pst.executeUpdate();

			LOGGER.info("INSERT LOG OK!!!");
			return true;

		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.closePreparedStatement(pst);
			DBUtil.closeConn(conn);
		}
		return false;
	}

}
