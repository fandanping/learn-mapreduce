package com.neusoft.statistics;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class StatisticData {
	
	
	public static class StatisticMapper extends Mapper<Object, Text, Text, Text>{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String date = line.split(" ")[0];
			int index = line.indexOf("url");
			String module = line.substring(index+4);//ȥ�� url:
			context.write(new Text(date), new Text(module));
		}	
	}
	
	public static class StatisticReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> urls, Context context) throws IOException, InterruptedException{
			Map<Text, Integer> map = new HashMap<Text, Integer>();
			for(Text url: urls){
				int n = map.get(url);
				if(n !=0){
					map.put(url, n + 1);
				}else{
					map.put(url, 0);
				}
			}
			for(Text k: map.keySet()){
				context.write(key, new Text(k.toString() + "     " + map.get(k)));
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    String[] otherArgs =new GenericOptionsParser(args).getRemainingArgs();
	    /**
	     * �������������/���
	     */
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: wordcount <in> <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "pv url");
	    job.setJarByClass(StatisticData.class);//����
	    
	    job.setMapperClass(StatisticMapper.class);//mapper
	    job.setCombinerClass(StatisticReducer.class);//��ҵ�ϳ���
	    job.setReducerClass(StatisticReducer.class);//reducer
	    
	    job.setOutputKeyClass(Text.class);//������ҵ������ݵĹؼ���
	    job.setOutputValueClass(IntWritable.class);//������ҵ���ֵ��
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//�ļ�����
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//�ļ����
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);//�ȴ�����˳�.

	}

}
