package com.tcdr;

import java.io.IOException;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
//import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CallDataRecord {
	public static class CDRMapper extends Mapper<Object, Text, Text, LongWritable>{
		private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] fields = value.toString().split("\\|");//split each line of input data into fields
			
			//get value of fromPhoneNumber, callStartTime, callEndTime, stdFlag
			String fromPhoneNumber = fields[0];
			Date callStartTime;
			Date callEndTime;
			
			try {
				callStartTime = sdf.parse(fields[2]);
				callEndTime = sdf.parse(fields[3]);
			} catch (ParseException e) {
				e.printStackTrace();
				return;
			}
			
			int stdFlag = Integer.parseInt(fields[4]);
			
			long duration = 0;//initialize duration variable
			
			//check stdFlag
			if (stdFlag == 1) {
				duration = (callEndTime.getTime() - callStartTime.getTime()) / 1000; // Convert milliseconds to seconds
				context.write(new Text(fromPhoneNumber), new LongWritable(duration));
			}
			
		}
	}
	
	public static class CDRReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long totalDuration = 0;//initialize totalDuration variable
			
			//get totalDuration of each phone number
			for (LongWritable value: values) {
				totalDuration += value.get();
			}
			
			//checl totalDuration
			if (totalDuration >= 3600) {
				context.write(key, new LongWritable(totalDuration));
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CDR Analytics");
        
        job.setJarByClass(CallDataRecord.class);
        job.setMapperClass(CDRMapper.class);
        job.setCombinerClass(CDRReducer.class);
        job.setReducerClass(CDRReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
