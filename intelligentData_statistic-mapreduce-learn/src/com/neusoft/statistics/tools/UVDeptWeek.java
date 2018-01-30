package com.neusoft.statistics.tools;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import com.neusoft.statistics.utils.ConfigContextUtils;
import com.neusoft.statistics.utils.SubstringUtils;


/**
 * @author zhengchj
 * @Email zhengchj@neusoft.com 
 * @Description: 统计每周各个部门,各个领域用户访问次数
 *
 */
public class UVDeptWeek implements Tool {
	
	
	public static class UVDeptWeekMapper extends Mapper<Object, Text, Text, Text>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String dateWithHour = SubstringUtils.getLogDateTimeHour(line);
			String sunday = SubstringUtils.getDayOfWeek(1);
			String friday = SubstringUtils.getDayOfWeek(6);
			if((dateWithHour.compareTo(sunday + " 17") > 0) && (dateWithHour.compareTo(friday + " 17") < 0)){
				String userId = SubstringUtils.getLogUserId(line);
				context.write(new Text("date"), new Text(userId));
			}
		}
		
	}
	
	
	public static class UVDeptWeekReducer extends Reducer<Text, Text, Text, Text>{

		private Connection conn = null;
		private Statement stmt = null;
		private String ids = "";
		private int i = 1000;
		private static final String QUERY_DEPT = "SELECT USER_ID,DEPT_NAME,AREA_NAME FROM SIPO_PROSEARCH.VIEW_AREA_DEPT@SIPO_PROSEARCH WHERE USER_ID IN(REPLACE)";
		@Override
		protected void reduce(Text key, Iterable<Text> userIds, Context context)
				throws IOException, InterruptedException {
			Map<String, Integer> userMap = new HashMap<String, Integer>();
			for(Text userId: userIds){
				userMap.put(userId.toString(), 1);
			}
			
			Map<String, Integer> deptMap = new HashMap<String, Integer>();
			Map<String, Integer> areaMap = new HashMap<String, Integer>();
			for(String id: userMap.keySet()){
				if(i != 0){
					ids = ids + ",'" + id + "'";
					i--;
				}else{
					getData(deptMap, areaMap);
				}
			}
			if(!ids.equals("")){
				getData(deptMap, areaMap);
			}
			for(String deptName: deptMap.keySet()){
				context.write(new Text(deptName), new Text(deptMap.get(deptName).toString()));
			}
		    context.write(new Text("--------------"), new Text("--------------"));
			for(String areaName: areaMap.keySet()){
				context.write(new Text(areaName), new Text(areaMap.get(areaName).toString()));
			}
		}
		
		private void getData(Map<String, Integer> deptMap, Map<String, Integer> areaMap){
			ResultSet rs = null;
			try {
				String sql = QUERY_DEPT.replace("REPLACE", ids.substring(1));
				rs = this.stmt.executeQuery(sql);
				while(rs.next()){
					String deptName = rs.getString("DEPT_NAME");
					String areaName = rs.getString("AREA_NAME");
					if(deptMap.get(deptName) != null){
						int n = deptMap.get(deptName);
						deptMap.put(deptName, n + 1);
					}else{
						deptMap.put(deptName, 1);
					}
					if(areaMap.get(areaName) != null){
						int m = areaMap.get(areaName);
						areaMap.put(areaName, m + 1);
					}else{
						areaMap.put(areaName, 1);
					}
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally{
				ids = "";
				i = 1000;
				try {
					rs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			try {
				this.stmt.close();
				this.conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			try{
				Class.forName(ConfigContextUtils.getInstance().getProperty("DRIVAERCLASS"));
				conn = DriverManager.getConnection(ConfigContextUtils.getInstance().getProperty("DBURL"),
						ConfigContextUtils.getInstance().getProperty("DBUSERNAME"), ConfigContextUtils.getInstance().getProperty("DBPASSWD"));
				this.stmt = this.conn.createStatement();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			super.setup(context);
		}
		
	}
	
	

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    String[] otherArgs =new GenericOptionsParser(args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	    	System.err.println("Usage: wordcount <in> <out>");
		    System.exit(2);
	    }
	    DistributedCache.addFileToClassPath(new Path(ConfigContextUtils.getInstance().getProperty("ojdbcJarPath")), conf);
		Job job = new Job(conf, "UVDeptWeek");
		job.setJarByClass(UVDeptTool.class);//主类
		    
		job.setMapperClass(UVDeptWeekMapper.class);//mapper
		//job.setCombinerClass(UVPerDayReducer.class);//作业合成类
		job.setReducerClass(UVDeptWeekReducer.class);//reducer
		    
		job.setOutputKeyClass(Text.class);//设置作业输出数据的关键类
		job.setOutputValueClass(Text.class);//设置作业输出值类
		    
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputDirRecursive(job, true);
		for(int i = 1; i<=6; i++){
			FileInputFormat.addInputPath(job, new Path(otherArgs[0] + SubstringUtils.getDayOfWeek(i) + "/"));//文件输入
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//文件输出
		    
		boolean result = job.waitForCompletion(true);
		return result ? 0 : 1;
	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setConf(Configuration arg0) {
		// TODO Auto-generated method stub

	}

}
