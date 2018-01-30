package com.neusoft.statistics.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author zhengchj
 * @Email zhengchj@neusoft.com 
 * @Description: ����־��ȡ���ݹ�����
 *
 */
public class SubstringUtils {
	
	//public static String NOW = getDateByFormat("yyyy-MM-dd");
	
	public static String NOW = getDateByFormat("yyyy-MM-dd");
	
	
	/**
	 * <p>[����־��ȡ����]</p>
	 * 
	 * @param line   ������־����
	 * @return: String
	 * @author: zhengchj
	 * @mail: zhengchj@neusoft.com
	 * @date: Created on 2017-8-29 ����09:17:41
	 */
	public static String getLogDate(String line){
		return line.substring(0, 10);
	}
	/**
	 * <p>[����־��ȡ�û�ID]</p>
	 * 
	 * @param line   ������־����
	 * @return: String
	 * @author: zhengchj
	 * @mail: zhengchj@neusoft.com
	 * @date: Created on 2017-8-29 ����09:18:22
	 */
	public static String getLogUserId(String line){
		int start = line.indexOf("user");
		int end = line.indexOf(";", start);
		return line.substring(start + 5, end);
	}
	
	
	/**
	 * <p>[����־��ȡ���ڡ�ʱ��]</p>
	 * 
	 * @param line   ������־����
	 * @return: String
	 * @author: zhengchj
	 * @mail: zhengchj@neusoft.com
	 * @date: Created on 2017-8-29 ����11:46:18
	 */
	public static String getLogDateTimeMinute(String line){
		return line.substring(0, 16);
	}
	
	/**
	 * <p>[����־��ȡ���ڡ�ʱ�䣨ֻ������Сʱ��]</p>
	 * 
	 * @param line
	 * @return: String
	 * @author: zhengchj
	 * @mail: zhengchj@neusoft.com
	 * @date: Created on 2017-8-29 ����05:30:56
	 */
	public static String getLogDateTimeHour(String line){
		return line.substring(0, 13);
	}
	
	/**
	 * <p>[���ݸ�ʽ�����ǰʱ���ַ���]</p>
	 * 
	 * @param format    yyyy-MM-dd�ȵ�
	 * @return: String
	 * @author: zhengchj
	 * @mail: zhengchj@neusoft.com
	 * @date: Created on 2017-8-29 ����11:52:52
	 */
	public static String getDateByFormat(String format){
		SimpleDateFormat df = new SimpleDateFormat(format);
		return df.format(new Date());
	}
	
	/**
	 * <p>[��ȡurl action��]</p>
	 * 
	 * @param line
	 * @return: String
	 * @author: zhengchj
	 * @mail: zhengchj@neusoft.com
	 * @date: Created on 2017-8-29 ����06:04:33
	 */
	public static String getAction(String line){
		Pattern p = Pattern.compile("[^/]+.do");
		Matcher m = p.matcher(line);
		String s = "";
		if(m.find()){
			s = m.group(0);
		}
		return s;
	}
	
	/**
	 * <p>[��ȡ��������]</p>
	 * 
	 * @return
	 * @return: String
	 * @author: zhengchj
	 * @mail: zhengchj@neusoft.com
	 * @date: Created on 2017-9-11 ����10:35:48
	 */
	public static String getYesterday(){
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, -1);
		String yesterday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime());
		return yesterday;
	}
	
	/**
	 * <p>[��ȡ��ǰ�ܵ�ĳһ�������]</p>
	 * 
	 * @return
	 * @return: String
	 * @author: zhengchj
	 * @mail: zhengchj@neusoft.com
	 * @date: Created on 2017-9-11 t s����09:50:39
	 */
	public static String getDayOfWeek(int index){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");        
	    Calendar cal = Calendar.getInstance();
	    cal.set(Calendar.DAY_OF_WEEK, index);
		return sdf.format(cal.getTime());
	}
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String str = "2017-08-30 16:17:46,475;user:437581;an:2015102359390;ip:10.79.10.105;url:/intelligent/intelligentbase/similar/similar-sort-literature.do";
		String str1 = "2017-08-27 22:46:43,346user:621517;ip:10.79.180.128;url:/intelligent/intelligentSearch/case-information.do";
		System.out.println(getDayOfWeek(5));
	}

}
