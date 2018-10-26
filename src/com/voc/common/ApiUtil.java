package com.voc.common;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

public class ApiUtil {
	private static final SimpleDateFormat SDF_DAILY = new SimpleDateFormat("yyyy-MM-dd");
	private static final SimpleDateFormat SDF_MONTHLY = new SimpleDateFormat("yyyy-MM");
	
	/**
	 * Get Daily Date String List
	 * 
	 * @param startDate yyyy-MM-dd
	 * @param endDate yyyy-MM-dd
	 * @return dailyList yyyy-MM-dd
	 */
	public static List<String> getDailyList(String startDate, String endDate) {
		String[] startArr = startDate.split("-");
		String[] endArr = endDate.split("-");
		
		List<String> datesInRange = new ArrayList<>();
		
		Calendar calendar = new GregorianCalendar();
		calendar.set(Integer.parseInt(startArr[0]), (Integer.parseInt(startArr[1])-1), Integer.parseInt(startArr[2]));
		
		Calendar endCalendar = new GregorianCalendar();
		endCalendar.set(Integer.parseInt(endArr[0]), (Integer.parseInt(endArr[1])-1), Integer.parseInt(endArr[2]));
		
		while (calendar.before(endCalendar) || calendar.equals(endCalendar)) {
			Date result = calendar.getTime();
			datesInRange.add(SDF_DAILY.format(result));
			calendar.add(Calendar.DATE, 1);
		}
		return datesInRange;
	}
	
	/**
	 * Get Monthly Date String List
	 * 
	 * @param startYearMonth yyyy-MM
	 * @param endYearMonth yyyy-MM
	 * @return monthlyList yyyy-MM
	 */
	public static List<String> getMonthlyList(String startYearMonth, String endYearMonth) {
		String[] startArr = startYearMonth.split("-");
		String[] endArr = endYearMonth.split("-");
		
		List<String> datesInRange = new ArrayList<>();
		
		Calendar calendar = new GregorianCalendar();
		calendar.set(Calendar.YEAR, Integer.parseInt(startArr[0]));
		calendar.set(Calendar.MONTH, Integer.parseInt(startArr[1]) - 1);
		
		Calendar endCalendar = new GregorianCalendar();
		endCalendar.set(Calendar.YEAR, Integer.parseInt(endArr[0]));
		endCalendar.set(Calendar.MONTH, Integer.parseInt(endArr[1]) - 1);
		
		while (calendar.before(endCalendar) || calendar.equals(endCalendar)) {
			Date result = calendar.getTime();
			datesInRange.add(SDF_MONTHLY.format(result));
			calendar.add(Calendar.MONTH, 1);
		}
		return datesInRange;
	}
	
	// --------------------------------------------------------------------
	
	public static JSONArray getDailyArray(String startDate, String endDate) {
		JSONArray dailyArray = new JSONArray();
		List<String> dailyList = getDailyList(startDate, endDate);
		for (String dateStr : dailyList) {
			JSONObject dailyObject = new JSONObject();
			dailyObject.put("date", dateStr);
			dailyObject.put("count", 0);
			dailyArray.put(dailyObject);
		}
		return dailyArray;
	}

	public static JSONArray getMonthlyArray(String startDate, String endDate) {
		JSONArray monthlyArray = new JSONArray();
		List<String> monthlyList = getMonthlyList(startDate, endDate);
		for (String dateStr : monthlyList) {
			JSONObject monthlyObject = new JSONObject();
			monthlyObject.put("date", dateStr);
			monthlyObject.put("count", 0);
			monthlyArray.put(monthlyObject);
		}
		return monthlyArray;
	}
	
	
	//*************************************************************

	/**
	 * Test Examples:
	 */
	public static void main(String[] args) {
		test_1();
		System.out.println("================================");
		test_2();
	}
	
	public static void test_1() {
		List<String> dailyList = ApiUtil.getDailyList("2018-12-16", "2019-03-17");
		for(String dateStr : dailyList) {
			System.out.println(dateStr);
		}
	}
	
	public static void test_2() {
		List<String> monthlyList = ApiUtil.getMonthlyList("2018-12", "2019-03");
		for(String yearMonth : monthlyList) {
			System.out.println(yearMonth);
		}
	}

}
