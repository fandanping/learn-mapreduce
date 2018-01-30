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

import com.neusoft.statistics.utils.SubstringUtils;


/**
 * @author zhengchj
 * @Email zhengchj@neusoft.com 
 * @Description: ͳ�Ʒ�����߷��ʴ��������ͷ��ʴ������ƽ����ʴ���
 *
 */
public class UVPerMinuteTool implements Tool {
	
	public static class UVPerMinuteMapper extends Mapper<Object, Text, Text, Text>{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();  //得到输入的每一行数据
			String dateWithMinute = SubstringUtils.getLogDateTimeMinute(line);  //分割
			String nowDate = SubstringUtils.NOW;
			if((dateWithMinute.compareTo(nowDate + " 08:00") > 0) && (dateWithMinute.compareTo(nowDate + " 17:00") < 0)){
				String userId = SubstringUtils.getLogUserId(line);
				context.write(new Text(nowDate), new Text(userId + "@" + dateWithMinute));//输出
			}
			
		}	
	}
	
	public static class UVPerMinuteReducer extends Reducer<Text, Text, Text, Text>{
		//reduce方法接收的是：一个字符串类型的key、一个可迭代的数据集
		//reduce方法接收到的是同一个key的一组value。
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
			context.write(key, new Text("max of per minute:" + max + "   min of per minute:" + min + "   avg per minute:" + avg));
			
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		//创建配置对象
		Configuration conf = new Configuration();
	    String[] otherArgs =new GenericOptionsParser(args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	    	System.err.println("Usage: wordcount <in> <out>");
		    System.exit(2);
	    }
	     //创建Job对象
		Job job = new Job(conf, "UVPerMinute");
		//设置运行JOB的类
		job.setJarByClass(UVPerMinuteTool.class);//����
		//  设置运行Mapper的类  
		job.setMapperClass(UVPerMinuteMapper.class);//mapper
		//job.setCombinerClass(UVPerDayReducer.class);//��ҵ�ϳ���
		//  设置运行Redece的类  
		job.setReducerClass(UVPerMinuteReducer.class);//reducer
		//设置map输出的key和value    
		job.setOutputKeyClass(Text.class);//������ҵ�����ݵĹؼ���
		job.setOutputValueClass(Text.class);//������ҵ���ֵ��
		//设置reduce输出的key和value   
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputDirRecursive(job, true);
		//设置输入输出的路径
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//�ļ�����
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//�ļ����
		//提交Job    
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
