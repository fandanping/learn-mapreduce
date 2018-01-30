package com.neusoft.statistics.tools;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import com.neusoft.statistics.tools.UVPerMinuteTool.UVPerMinuteMapper;
import com.neusoft.statistics.tools.UVPerMinuteTool.UVPerMinuteReducer;
import com.neusoft.statistics.utils.SubstringUtils;


/**
 * @author zhengchj
 * @Email zhengchj@neusoft.com 
 * @Description: 统计小时最高访问次数、小时最低访问次数、小时平均访问次数
 *
 */
public class UVPerHourTool implements Tool {
	
	public static class UVPerHourMapper extends Mapper<Object, Text, Text, Text>{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String dateWithHour = SubstringUtils.getLogDateTimeHour(line);
			String nowDate = SubstringUtils.NOW;
			if((dateWithHour.compareTo(nowDate + " 08") > 0) && (dateWithHour.compareTo(nowDate + " 17") < 0)){
				String userId = SubstringUtils.getLogUserId(line);
				context.write(new Text(nowDate), new Text(userId + "@" + dateWithHour));
			}
			
		}	
	}
	
	public static class UVPerHourReducer extends Reducer<Text, Text, Text, Text>{
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			Map<String, Map<String, Integer>> map = new HashMap<String, Map<String, Integer>>();
			for(Text item: values){
				String[] arr = item.toString().split("@");
				String userId = arr[0];
				String time = arr[1];
				if(map.get(time) != null){
					Map<String, Integer> userMap = map.get(time);
					userMap.put(userId, 1);
				}else{
					Map<String, Integer> userMap = new HashMap<String, Integer>();
					userMap.put(userId, 1);
					map.put(time, userMap);
				}
			}
			int max = Integer.MIN_VALUE, min = Integer.MAX_VALUE, sum = 0, avg = 0;
			for(String k: map.keySet()){
				int size = map.get(k).size();
				max = Math.max(max, size);
				min = Math.min(min, size);
				sum = sum + size;
			}
			avg = sum / map.size();
			context.write(key, new Text("max of per hour:" + max + "   min of per hour:" + min + "   avg per hour:" + avg));
			
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
		Job job = new Job(conf, "UVPerHour");
		job.setJarByClass(UVPerHourTool.class);//主类
		    
		job.setMapperClass(UVPerHourMapper.class);//mapper
		//job.setCombinerClass(UVPerDayReducer.class);//作业合成类
		job.setReducerClass(UVPerHourReducer.class);//reducer
		    
		job.setOutputKeyClass(Text.class);//设置作业输出数据的关键类
		job.setOutputValueClass(Text.class);//设置作业输出值类
		    
		FileInputFormat.setInputDirRecursive(job, true);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		    
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//文件输入
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
