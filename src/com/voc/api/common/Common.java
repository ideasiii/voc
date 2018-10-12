package com.voc.api.common;
import java.util.Calendar;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParseException;

public class Common {

	public final static int ERR_SUCCESS = 1;
	public final static int ERR_EXCEPTION = -1;
	
	//private static final String DB_IP = "127.0.0.1";
	private static final String RDS_IP = "ggininder-1-cluster.cluster-cgcqh1oe3kxf.ap-northeast-1.rds.amazonaws.com";
	public static final String DB_URL_VOC = "jdbc:mysql://" + RDS_IP
			+ ":3306/ibuzz_voc?useUnicode=true&characterEncoding=UTF-8&useSSL=false&verifyServerCertificate=false";
	public static final String DB_USER_RDS = "ibuzz_voc";
	public static final String DB_PASS_RDS = "ibuzz_voc123!";
	
	
	
	
	
	/** VALIDATIONS **/
	
	public static boolean isValidDate(String dateToValidate, String dateFromat) {

		if (dateToValidate == null) {
			return false;
		}

		SimpleDateFormat sdf = new SimpleDateFormat(dateFromat);
		sdf.setLenient(false);

		try {
			//if not valid, it will throw ParseException
			Date date = sdf.parse(dateToValidate);
			//System.out.println(date);

		} catch (ParseException e) {

			e.printStackTrace();
			return false;
		}

		return true;
	}
	
	 public static boolean isNotEmptyString(final String s) {
	    	return s != null && s.length() > 0;
	    }

	public static boolean isValidStartDate(String sd, String ed, String dateFromat) {

		SimpleDateFormat sdf = new SimpleDateFormat(dateFromat);
		sdf.setLenient(false);

		try {
		
			//check startDate before or equal endDate 
			boolean validStartDate = !sdf.parse(sd).after(sdf.parse(ed));
			
			if (validStartDate == false)
				return false;
			
		} catch (ParseException e) {

			e.printStackTrace();
			return false;
		}

		return true;
	}

	public static boolean isValidDateInSameMonth(final String sd, final String ed, String dateFromat) {

		SimpleDateFormat sdf = new SimpleDateFormat(dateFromat);
		sdf.setLenient(false);

		try {
			Date date1 = sdf.parse(sd);
			Date date2 = sdf.parse(ed);

			Calendar c1 = Calendar.getInstance();
			Calendar c2 = Calendar.getInstance();
			c1.setTime(date1);
			c2.setTime(date2);

			boolean sameMonth = c1.get(Calendar.MONTH) == c2.get(Calendar.MONTH);
			if (sameMonth == false)
				return false;

		} catch (ParseException e) {

			e.printStackTrace();
			return false;
		}

		return true;
	}
	
	
	
	
	
}


