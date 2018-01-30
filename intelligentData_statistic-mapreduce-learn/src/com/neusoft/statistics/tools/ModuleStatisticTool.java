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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import com.neusoft.statistics.StatisticData;
import com.neusoft.statistics.utils.ConfigContextUtils;
import com.neusoft.statistics.utils.SubstringUtils;

/**
 * @author zhengchj
 * @Email zhengchj@neusoft.com 
 * @Description: 统计当天各个模块访问量
 *
 */
public class ModuleStatisticTool implements Tool {
	
	public static class ModuleStatisticMapper extends Mapper<Object, Text, Text, Text>{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String date = SubstringUtils.getLogDate(line);
			String dateWithHour = SubstringUtils.getLogDateTimeHour(line);
			String nowTime = SubstringUtils.NOW;
			String yesterday = SubstringUtils.getYesterday();
			if((dateWithHour.compareTo(yesterday + " 17") > 0) && (dateWithHour.compareTo(nowTime + " 17") < 0)){
				String action = SubstringUtils.getAction(line);
				String userId = SubstringUtils.getLogUserId(line);
				if(!action.equals("")){
					String moduleName = ConfigContextUtils.getInstance().getProperty(action);
					String name = moduleName!=null?moduleName:action;
					context.write(new Text(name), new Text(userId));
				}
			}
			
		}	
	}
	
	public static class ModuleStatisticReducer extends Reducer<Text, Text, Text, Text>{
		
		public void reduce(Text key, Iterable<Text> userIds, Context context) throws IOException, InterruptedException{
			Map<String, Integer> userMap = new HashMap<String, Integer>();
			for(Text userId: userIds){
				userMap.put(userId.toString(), 1);
			}
			Integer size = userMap.size();
			context.write(key, new Text(size.toString()));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    String[] otherArgs =new GenericOptionsParser(args).getRemainingArgs();
	    /**
	     * 这里必须有输入/输出
	     */
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: wordcount <in> <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "moduleStatistic");
	    job.setJarByClass(StatisticData.class);//主类
	    
	    job.setMapperClass(ModuleStatisticMapper.class);//mapper
	    //job.setCombinerClass(ModuleStatisticReducer.class);//作业合成类
	    job.setReducerClass(ModuleStatisticReducer.class);//reducer
	    
	    job.setOutputKeyClass(Text.class);//设置作业输出数据的关键类
	    job.setOutputValueClass(Text.class);//设置作业输出值类
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    FileInputFormat.setInputDirRecursive(job, true);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0] + SubstringUtils.getYesterday() + "/"));//文件输入
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0] + SubstringUtils.getDateByFormat("yyyy-MM-dd") + "/"));//文件输入
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//文件输出
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);//等待完成退出.
	    
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
