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
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.voc.api.RootAPI;
import com.voc.api.model.ChannelRankingModel;
import com.voc.common.ApiResponse;
import com.voc.common.Common;
import com.voc.common.DBUtil;

/**
 * 查詢時間區間內觀測的品牌中各頻道的口碑總量排名。
 * 
 * Method	URL
 * GET	/industry/channel-ranking.jsp
 * 
 * Type	Name	Values	Description
 * query	*brand	    string	欲查詢品牌
 * query	*channel	string	欲查詢來源頻道
 * query	*start_date	string	欲查詢起始日期
 * query	*end_date	string	欲查詢結束日期
 * query	 sort	    string	排序方式，升冪或降冪排序(asc、desc) Default: desc
 * query	 limit	    string	顯示項目筆數 Default: 10
 * query	 api_key	string	MORE API token
 * 
 * EX: SQL:
 * SELECT website_name, channel_name, channel_id, SUM(reputation) AS count 
 * FROM ibuzz_voc.brand_reputation 
 * WHERE brand IN ('BENZ', 'BMW') AND channel_id IN ('5ad450dca85d0a2afac34956', '5ad450dea85d0a2afac34aa8') 
 * AND DATE_FORMAT(date, '%Y-%m-%d') >= '2018-04-15' AND DATE_FORMAT(date, '%Y-%m-%d') <= '2018-05-30' 
 * GROUP BY website_id, channel_id ORDER BY count DESC LIMIT 10;
 * 
 * EX: API URL:
 * http://localhost:8080/voc/industry/channel-ranking.jsp?brand=BENZ;BMW&channel=5ad450dca85d0a2afac34956;5ad450dea85d0a2afac34aa8&start_date=2018-04-15&end_date=2018-05-30&sort=DESC&limit=10
 * http://localhost:8080/voc/industry/channel-ranking.jsp?brand=BENZ;BMW&channel=5ad450dca85d0a2afac34956;5ad450dea85d0a2afac34aa8&start_date=2018-04-15&end_date=2018-05-30&sort=ASC&limit=15
 * http://localhost:8080/voc/industry/channel-ranking.jsp?brand=BENZ;BMW&channel=5ad450dca85d0a2afac34956;5ad450dea85d0a2afac34aa8&start_date=2018-04-15&end_date=2018-05-30
 * 
 */
public class ChannelRanking extends RootAPI {
	private static final Logger LOGGER = LoggerFactory.getLogger(ChannelRanking.class);
	private static final Gson GSON = new Gson();
	private Map<String, String[]> parameterMap;
	private String brand;
	private String channel;
	private String start_date;
	private String end_date;
	private String sort = Common.SORT_DESC; // Default: desc
	private int limit = 10; // Default: 10
	
	private String tableName;
	private String[] brandValueArr = null; // brand
	private String[] channelValueArr = null; // channel (channel_id): channel_id若不存在, 就不要顯示結果
	private Map<String, String> hash_channelId_websiteChannelName = new HashMap<>();
	
	@Override
	public String processRequest(HttpServletRequest request) {
		this.requestAndTrimParams(request);
		JSONObject errorResponse = this.validateParams();
		if (errorResponse != null) {
			return errorResponse.toString();
		}
		List<ChannelRankingModel.Channel> channelList = this.queryData();
		if (channelList != null) {
			ChannelRankingModel channelRankingModel = new ChannelRankingModel();
			channelRankingModel.setSuccess(true);
			channelRankingModel.setResult(channelList);
			String responseJsonStr = GSON.toJson(channelRankingModel);
			LOGGER.info("responseJsonStr=" + responseJsonStr);
			return responseJsonStr;
		}
		return ApiResponse.unknownError().toString();
	}

	private void requestAndTrimParams(HttpServletRequest request) {
		this.parameterMap = request.getParameterMap();
		this.brand = StringUtils.trimToEmpty(request.getParameter("brand"));
		this.channel = StringUtils.trimToEmpty(request.getParameter("channel"));
		this.start_date = StringUtils.trimToEmpty(request.getParameter("start_date"));
		this.end_date = StringUtils.trimToEmpty(request.getParameter("end_date"));
		
		String sort = StringUtils.trimToEmpty(request.getParameter("sort"));
		if (!StringUtils.isEmpty(sort)) {
			this.sort = sort;
		}
		
		String limitStr = StringUtils.trimToEmpty(request.getParameter("limit"));
		if (!StringUtils.isEmpty(limitStr)) {
			try {
				this.limit = Integer.parseInt(limitStr);
			} catch (Exception e) {
				LOGGER.error(e.getMessage());
			}
		}
		
		this.tableName = this.getTableName(this.parameterMap);
		this.brandValueArr = this.brand.split(PARAM_VALUES_SEPARATOR);
		this.channelValueArr = this.channel.split(PARAM_VALUES_SEPARATOR);
		for (int i = 0; i < channelValueArr.length; i++) {
			String channelId = channelValueArr[i];
			// String websiteChannelName = this.getWebsiteChannelNameById(this.tableName, channelId); 
			String websiteChannelName = this.getWebsiteChannelNameById(channelId); // TBD: thinking...
			if (!StringUtils.isBlank(websiteChannelName)) {
				this.hash_channelId_websiteChannelName.put(channelId, websiteChannelName);
			} else {
				LOGGER.error("Invalid channelId=" + channelId);
			}
		}
	}

	private JSONObject validateParams() {
		if (StringUtils.isBlank(this.brand) || StringUtils.isBlank(this.channel)
				|| StringUtils.isBlank(this.start_date) || StringUtils.isBlank(this.end_date)) {

			return ApiResponse.error(ApiResponse.STATUS_MISSING_PARAMETER);
		}
		
		if (!Common.isValidDate(this.start_date, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid start_date.");
		}
		this.start_date = Common.formatDate(this.start_date, "yyyy-MM-dd");
		
		if (!Common.isValidDate(this.end_date, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid end_date.");
		}
		this.end_date = Common.formatDate(this.end_date, "yyyy-MM-dd");
		
		if (!Common.isValidStartDate(this.start_date, this.end_date, "yyyy-MM-dd")) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid period values.");
		}
		
		if (!this.sort.equalsIgnoreCase(Common.SORT_ASC) && !this.sort.equalsIgnoreCase(Common.SORT_DESC)) {
			return ApiResponse.error(ApiResponse.STATUS_INVALID_PARAMETER, "Invalid sort value.");
		}
		return null;
	}

	private List<ChannelRankingModel.Channel> queryData() {
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		try {
			conn = DBUtil.getConn();
			preparedStatement = conn.prepareStatement(this.genSelectSQL());
			this.setWhereClauseValues(preparedStatement);
			
			String psSQLStr = preparedStatement.toString();
			LOGGER.debug("psSQLStr = " + psSQLStr);
			
			List<String> channelIdList = new ArrayList<>();
			List<ChannelRankingModel.Channel> channelList = new ArrayList<>();
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				// website_name, channel_name, channel_id, SUM(reputation) AS count
				String websiteName = rs.getString("website_name");
				String channelName = rs.getString("channel_display_name");
				String channelId = rs.getString("channel_id");
				int count = rs.getInt("count");
				LOGGER.debug("websiteName=" + websiteName + ", channelName=" + channelName + ", channelId=" + channelId + ", count=" + count);
				
				channelIdList.add(channelId);
				
				ChannelRankingModel.Channel channel = new ChannelRankingModel().new Channel();
				channel.setChannel_id(channelId);
				channel.setChannel(channelName);
				channel.setCount(count);
				channelList.add(channel);
			}
			LOGGER.debug("channelList_before=" + GSON.toJson(channelList));

			// TODO:==>TBD: need to think and test: ... 
			int desc_remainingCnt = this.limit - channelList.size();
			int asc_recordCnt = 0;
			for (Map.Entry<String, String> entry : this.hash_channelId_websiteChannelName.entrySet()) {
				String channelId = entry.getKey();
				String websiteChannelName = entry.getValue();
				if (!channelIdList.contains(channelId)) {
					ChannelRankingModel.Channel channel = new ChannelRankingModel().new Channel();
					channel.setChannel_id(channelId);
					channel.setChannel(websiteChannelName);
					channel.setCount(0);
					if (this.sort.equalsIgnoreCase(Common.SORT_DESC)) {
						if (desc_remainingCnt > 0) {
							channelList.add(channel);
							desc_remainingCnt--;
						}
					} else if (this.sort.equalsIgnoreCase(Common.SORT_ASC)) {
						if ((this.limit - asc_recordCnt) > 0) {
							channelList.add(0, channel);
							asc_recordCnt++;
						}
					}
				}
			}
			
			if (this.sort.equalsIgnoreCase(Common.SORT_ASC)) {
				int listSize = channelList.size();
				int recordSize = this.limit;
				if (this.limit > listSize) {
					recordSize = listSize;
				}
				channelList = channelList.subList(0, recordSize);
			}

			LOGGER.debug("channelList_after=" + GSON.toJson(channelList));
			return channelList;
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, preparedStatement, conn);
		}
		return null;
	}

	private String genSelectSQL() {
		StringBuffer selectSQL = new StringBuffer();
		selectSQL.append("SELECT website_name, channel_display_name, channel_id, SUM(reputation) AS count ");
		selectSQL.append("FROM ").append(this.tableName).append(" ");
		selectSQL.append("WHERE brand IN (");
		for(int i = 0 ; i < this.brandValueArr.length ; i++ ) {
			if (i == 0) selectSQL.append("?");
			else selectSQL.append(",?");
		}
		selectSQL.append(") ");
		selectSQL.append("AND channel_id IN (");
		for(int i = 0 ; i < this.channelValueArr.length ; i++ ) {
			if (i == 0) selectSQL.append("?");
			else selectSQL.append(",?");
		}
		selectSQL.append(") ");
		selectSQL.append("AND DATE_FORMAT(rep_date, '%Y-%m-%d') >= ? ");
		selectSQL.append("AND DATE_FORMAT(rep_date, '%Y-%m-%d') <= ? ");
		selectSQL.append("GROUP BY website_id, channel_id ORDER BY count ");
		if (this.sort.equalsIgnoreCase(Common.SORT_ASC)) {
			selectSQL.append(Common.SORT_ASC).append(" ");
		} else if (this.sort.equalsIgnoreCase(Common.SORT_DESC)) {
			selectSQL.append(Common.SORT_DESC).append(" ");
		}
		selectSQL.append("LIMIT ? ");
		return selectSQL.toString();
	}

	private void setWhereClauseValues(PreparedStatement preparedStatement) throws Exception {
		int idx = 0;
		for(String v : brandValueArr) {
			int parameterIndex = idx + 1;
			preparedStatement.setObject(parameterIndex, v);
			// LOGGER.debug(parameterIndex + ":" + v);
			idx++;
		}
		for(String v : channelValueArr) {
			int parameterIndex = idx + 1;
			preparedStatement.setObject(parameterIndex, v);
			// LOGGER.debug(parameterIndex + ":" + v);
			idx++;
		}
		
		int startDateIndex = idx + 1;
		preparedStatement.setObject(startDateIndex, this.start_date);
		// LOGGER.debug(startDateIndex + ":" + startDate);
		idx++;
		
		int endDateIndex = idx + 1;
		preparedStatement.setObject(endDateIndex, this.end_date);
		// LOGGER.debug(endDateIndex + ":" + endDate);
		idx++;
		
		int limitIdx = idx + 1;
		preparedStatement.setObject(limitIdx, this.limit);
		idx++;
	}
	
	private String getWebsiteChannelNameById(String id) {
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
	//	String websiteName = null;
		String channelName = null;
		String selectSql = "SELECT name FROM "+ TABLE_CHANNEL_LIST +" WHERE id = ? LIMIT 1";
		try {
			conn = DBUtil.getConn();
			preparedStatement = conn.prepareStatement(selectSql);
			preparedStatement.setString(1, id);
			rs = preparedStatement.executeQuery();
			if (rs.next()) {
				//websiteName = rs.getString("website_name");
				channelName = rs.getString("name");
			}
		//	if (websiteName != null && channelName != null) {
		//		return websiteName + "_" + channelName;
		//	}
			if (channelName != null) {
				return channelName;
				}
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(rs, preparedStatement, conn);
		}
		return null;
	}

//	private String getWebsiteChannelNameById(String tableName, String channelId) {
//		Connection conn = null;
//		PreparedStatement preparedStatement = null;
//		ResultSet rs = null;
//		String websiteName = null;
//		String channelName = null;
//		String selectSql = "SELECT website_name, channel_name FROM " + tableName + " WHERE channel_id = ? LIMIT 1";
//		try {
//			conn = DBUtil.getConn();
//			preparedStatement = conn.prepareStatement(selectSql);
//			preparedStatement.setString(1, channelId);
//			rs = preparedStatement.executeQuery();
//			if (rs.next()) {
//				websiteName = rs.getString("website_name");
//				channelName = rs.getString("channel_name");
//			}
//			if (websiteName != null && channelName != null) {
//				return websiteName + "_" + channelName;
//			}
//		} catch (Exception e) {
//			LOGGER.error(e.getMessage());
//			e.printStackTrace();
//		} finally {
//			DBUtil.close(rs, preparedStatement, conn);
//		}
//		return null;
//	}

}
