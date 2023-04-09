package com.mr;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemp {

  public static class TemperatureMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable temperature = new IntWritable();
    private Text year = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        String token = itr.nextToken();
        if (token.length() == 4) { // Check if token is a year
          year.set(token);
        } else { // Assume token is a temperature
          temperature.set(Integer.parseInt(token));
          context.write(year, temperature);
        }
      }
    }
  }

  public static class TemperatureReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int maxTemperature = Integer.MIN_VALUE;
      for (IntWritable val : values) {
        maxTemperature = Math.max(maxTemperature, val.get());
      }
      result.set(maxTemperature);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "max temperature");
    job.setJarByClass(MaxTemp.class);
    job.setMapperClass(TemperatureMapper.class);
    job.setCombinerClass(TemperatureReducer.class);
    job.setReducerClass(TemperatureReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}