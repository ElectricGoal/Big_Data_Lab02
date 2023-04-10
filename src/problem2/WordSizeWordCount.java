package com.mr;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordSizeWordCount {
  // Declare the Map class that inherits Mapper
  public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    // Declare variable one as IntWritable with value 1
    private final static IntWritable one = new IntWritable(1);
    // Declare variable zero as IntWritable with value 0
    private final static IntWritable zero = new IntWritable(0);

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      // Get the input stream
      String line = value.toString();
      // Split each word in the data stream with StringTokenizer
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        String word = tokenizer.nextToken().replaceAll("[^a-zA-Z0-9]", "");
        // Get the length of the word
        int length = word.length();
        // Write out the context of the word length and the value one
        context.write(new IntWritable(length), one);
        // Write out the context of the word length and the value zero
        context.write(new IntWritable(length), zero);
      }
    }
  }

  // Declare Reduce class to inherit Reducer
  public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int count = 0;
      // Iterate through the values in values
      for (IntWritable value : values) {
        // Add the value of value to the counter variable count
        count += value.get();
      }
      context.write(key, new IntWritable(count));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    // Initialize Job object with Configuration and job name "WordSize"
    Job job = Job.getInstance(conf, "WordSize");
    job.setJarByClass(WordSizeWordCount.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}