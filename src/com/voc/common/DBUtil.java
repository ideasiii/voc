package com.voc.common;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class DBUtil extends DB {
	
	public static Connection getConn() {
		Connection conn = connect(Common.DB_URL_VOC, Common.DB_USER_RDS, Common.DB_PASS_RDS);
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
	
}
