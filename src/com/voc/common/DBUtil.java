package com.voc.common;

import java.sql.Connection;

public class DBUtil extends DB {
	
	public static Connection getConn() {
		Connection conn = connect(Common.DB_URL_VOC, Common.DB_USER_RDS, Common.DB_PASS_RDS);
		return conn;
	}
	
}
