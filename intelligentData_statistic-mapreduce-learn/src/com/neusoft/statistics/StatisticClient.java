package com.neusoft.statistics;

import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.neusoft.statistics.tools.ModuleStatisticTool;
import com.neusoft.statistics.tools.UVDeptTool;
import com.neusoft.statistics.tools.UVDeptWeek;
import com.neusoft.statistics.tools.UVPerDayTool;
import com.neusoft.statistics.tools.UVPerHourTool;
import com.neusoft.statistics.tools.UVPerMinuteTool;
import com.neusoft.statistics.utils.SubstringUtils;

public class StatisticClient {
	
	private static final Logger log = LoggerFactory.getLogger(StatisticClient.class);
	
	private static final String OUTPUT_PATH = "/statistic/output";
	
	private static final String UV_PER_DAY_PATH = "/uvperday/";
	
	private static final String UV_PER_MINUTE_PATH = "/uvperminute/";
	
	private static final String UV_PER_HOUR_PATH = "/uvperhour/";
	
	private static final String MODULE_STATISTIC_PATH = "/modulestatistic/";
	
	private static final String UV_DEPT_PATH = "/uvdept/";
	
	private static final String UV_DEPT_WEEK_PATH = "/uvdeptweek/";
	
	private static final String INPUT_PATH = "/flume/webdata/";
	
	

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		String date = "";
		if(args.length == 3){
			date = args[2] + SubstringUtils.getDateByFormat("HHmmss");
		}else{
			date = SubstringUtils.getDateByFormat("yyyy-MM-ddHHmmss");
		}
		String type = args[0];
		String path = "";
		if(args.length >= 2){
			path = args[1];
		}else{
			path = INPUT_PATH;
		}
		log.info(path + "++++++++++");
		if(type.equals("UVPerDay")){
			UVPerDayTool upd = new UVPerDayTool();
			String[] updArg = {path, OUTPUT_PATH + UV_PER_DAY_PATH + date + "/"};
			ToolRunner.run(upd, updArg);
		}else if(type.equals("UVPerMinute")){
			UVPerMinuteTool upm = new UVPerMinuteTool();
			String[] upmArg = {path, OUTPUT_PATH + UV_PER_MINUTE_PATH + date + "/"};
			ToolRunner.run(upm, upmArg);
		}else if(type.equals("UVPerHour")){
			UVPerHourTool uph = new UVPerHourTool();
			String[] uphArg = {path, OUTPUT_PATH + UV_PER_HOUR_PATH + date + "/"};
			ToolRunner.run(uph, uphArg);
		}else if(type.equals("ModuleStatistic")){
			ModuleStatisticTool ms = new ModuleStatisticTool();
			String[] msArg = {path, OUTPUT_PATH + MODULE_STATISTIC_PATH + date + "/"};
			ToolRunner.run(ms, msArg);
		}else if(type.equals("UVDept")){
			UVDeptTool ud = new UVDeptTool();
			String[] udArg = {path, OUTPUT_PATH + UV_DEPT_PATH + date + "/"};
			ToolRunner.run(ud, udArg);
		}else if(type.equals("UVDeptWeek")){
			UVDeptWeek udw = new UVDeptWeek();
			String[] udwArg = {INPUT_PATH, OUTPUT_PATH + UV_DEPT_WEEK_PATH + date + "/"};
			ToolRunner.run(udw, udwArg);
		}else{
			log.info("no mapreduce to do it");
		}
		

	}

}
