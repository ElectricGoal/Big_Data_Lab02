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

  //Mapper class that maps each input record to a key-value pair
  public static class tempMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable temp = new IntWritable();
  //Defining a local variable year of type Text
    private Text year = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      //Split each record into tokens
      StringTokenizer Tokenizer = new StringTokenizer(value.toString());
      while (Tokenizer.hasMoreTokens()) {
        String token = Tokenizer.nextToken();
        //Check token is a year
        if (Integer.parseInt(token) >= 1000) { 
          year.set(token);
        } else { 
          temp.set(Integer.valueOf(token));
          //the key-value pair
          context.write(year, temp);
        }
      }
    }
  }

  //Reducer class that finds the maximum temp for each year
  public static class tempReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int maxtemp = -238;
      //Find the maximum temp
      for (IntWritable val : values) {
    	  if( maxtemp < val.get())
    	  {
    		  maxtemp = val.get();
    	  }
      }
      result.set(maxtemp);
      //the key-value pair
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "MaxTemp");
    job.setJarByClass(MaxTemp.class);
    job.setMapperClass(tempMapper.class);
    job.setCombinerClass(tempReducer.class);
    job.setReducerClass(tempReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    Path p=new Path(args[0]);
    Path p1=new Path(args[1]);
    FileInputFormat.addInputPath(job,p);
    FileOutputFormat.setOutputPath(job,p1);
    job.waitForCompletion(true);
  }
}