package com.voc.common;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

public class APIUtil {
	private static final SimpleDateFormat SDF_DAILY = new SimpleDateFormat("yyyy-MM-dd");
	private static final SimpleDateFormat SDF_MONTHLY = new SimpleDateFormat("yyyy-MM");
	
	/**
	 * Get daily Date String List
	 * 
	 * @param startDate yyyy-MM-dd
	 * @param endDate yyyy-MM-dd
	 * @return dailyList yyyy-MM-dd
	 */
	public static List<String> getDailyList(String startDate, String endDate) {
		String[] startDateArr = startDate.split("-");
		String[] endDateArr = endDate.split("-");
		
		List<String> datesInRange = new ArrayList<>();
		
		Calendar calendar = new GregorianCalendar();
		calendar.set(Integer.parseInt(startDateArr[0]), (Integer.parseInt(startDateArr[1])-1), Integer.parseInt(startDateArr[2]));
		
		Calendar endCalendar = new GregorianCalendar();
		endCalendar.set(Integer.parseInt(endDateArr[0]), (Integer.parseInt(endDateArr[1])-1), Integer.parseInt(endDateArr[2]));
		
		while (calendar.before(endCalendar) || calendar.equals(endCalendar)) {
			Date result = calendar.getTime();
			datesInRange.add(SDF_DAILY.format(result));
			calendar.add(Calendar.DATE, 1);
		}
		return datesInRange;
	}

	/**
	 * Get daily Date String List
	 * 
	 * @param startYearMonth yyyy-MM
	 * @param endYearMonth yyyy-MM
	 * @return monthlyList yyyy-MM
	 */
	public static List<String> getMonthlyList(String startYearMonth, String endYearMonth) {
		String[] startDateArr = startYearMonth.split("-");
		String[] endDateArr = endYearMonth.split("-");
		
		List<String> datesInRange = new ArrayList<>();
		
		Calendar calendar = new GregorianCalendar();
		calendar.set(Calendar.YEAR, Integer.parseInt(startDateArr[0]));
		calendar.set(Calendar.MONTH, Integer.parseInt(startDateArr[1]) - 1);
		
		Calendar endCalendar = new GregorianCalendar();
		endCalendar.set(Calendar.YEAR, Integer.parseInt(endDateArr[0]));
		endCalendar.set(Calendar.MONTH, Integer.parseInt(endDateArr[1]) - 1);
		
		while (calendar.before(endCalendar) || calendar.equals(endCalendar)) {
			Date result = calendar.getTime();
			datesInRange.add(SDF_MONTHLY.format(result));
			calendar.add(Calendar.MONTH, 1);
		}
		return datesInRange;
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
		List<String> dateList = getDailyList("2018-12-16", "2019-03-17");
		for(String dateStr : dateList) {
			System.out.println(dateStr);
		}
	}
	
	public static void test_2() {
		List<String> monthlyList = getMonthlyList("2018-12", "2019-03");
		for(String yearMonth : monthlyList) {
			System.out.println(yearMonth);
		}
	}

}
