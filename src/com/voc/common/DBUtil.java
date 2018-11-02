package com.voc.common;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.tomcat.dbcp.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voc.tool.AesEncryptor;

public class DBUtil extends DB {
	private static final Logger LOGGER = LoggerFactory.getLogger(DBUtil.class);
	private static BasicDataSource datasource = null;
	static {
		try {
			LOGGER.info("datasource init...");
			Context initialContext = new InitialContext();
			datasource = (BasicDataSource)initialContext.lookup("java:comp/env/jdbc/voc");
			
			String username = AesEncryptor.decrypt(datasource.getUsername());
			String password = AesEncryptor.decrypt(datasource.getPassword());
			datasource.setUsername(username);
			datasource.setPassword(password);
		} catch (NamingException e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		}
	}
	
	public static Connection getConn() throws Exception {
		// Connection conn = connect(Common.DB_URL_VOC, Common.DB_USER_RDS, Common.DB_PASS_RDS);
		Connection conn = datasource.getConnection();
		return conn;
	}
	
	public static int closeResultSet(ResultSet rs) {
	    try {
	        rs.close();
	    } catch (Exception e) {
	        e.printStackTrace();
	        return Common.ERR_EXCEPTION;
	    }
	    return Common.ERR_SUCCESS;
	}
	
	public static int closePreparedStatement(PreparedStatement ps) {
	    try {
	    	ps.close();
	    } catch (Exception e) {
	        e.printStackTrace();
	        return Common.ERR_EXCEPTION;
	    }
	    return Common.ERR_SUCCESS;
	}
	
	public static void close(ResultSet rs, PreparedStatement ps, Connection conn) {
		if (rs != null) {
			DBUtil.closeResultSet(rs);
		}
		if (ps != null) {
			DBUtil.closePreparedStatement(ps);
		}
		if (conn != null) {
			DBUtil.closeConn(conn);
		}
	}
	
}
