package com.neusoft.statistics;

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


public class UserStatisticData {
	
	
	public static class UserStatisticMapper extends Mapper<Object, Text, Text, Text>{

		//private Text dateText = new Text();
		//private Text user = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String date = line.split(" ")[0];
			if(date.equals("2017-08-29")){
				int start = line.indexOf("user");
				int end = line.indexOf(";ip");
				//dateText.set(date);
				//user.set();
				context.write(new Text(date), new Text(line.substring(start + 5, end)));
			}
		}	
	}
	
	public static class UserStatisticReducer extends Reducer<Text, Text, Text, Text>{
		
		public void reduce(Text key, Iterable<Text> users, Context context) throws IOException, InterruptedException{
			Map<String, String> map = new HashMap<String, String>();
			for(Text user: users){
				map.put(user.toString(), "a");
			}
			Integer size = map.keySet().size();
			context.write(key, new Text(size.toString()));
			
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    String[] otherArgs =new GenericOptionsParser(args).getRemainingArgs();
	    /**
	     * 这里必须有输入/输出
	     */
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: wordcount <in> <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "uv url");
	    job.setJarByClass(UserStatisticData.class);//主类
	    
	    job.setMapperClass(UserStatisticMapper.class);//mapper
	    //job.setCombinerClass(UserStatisticReducer.class);//作业合成类
	    job.setReducerClass(UserStatisticReducer.class);//reducer
	    
	    job.setOutputKeyClass(Text.class);//设置作业输出数据的关键类
	    job.setOutputValueClass(Text.class);//设置作业输出值类
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//文件输入
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//文件输出
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);//等待完成退出.

	}

}
