package patent.project;

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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

@SuppressWarnings("unused")
public class Patent {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	    Text k = new Text();
	    Text v = new Text();

	    @Override
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line, " ");
	        // Extracting the patent id as the key
	        String patentId = tokenizer.nextToken();
	        k.set(patentId);
	        // Extracting the sub-patent id as the value
	        String subPatentId = tokenizer.nextToken();
	        v.set(subPatentId);
	        // Sending to reducer
	        context.write(k, v);
	    }
	}

	public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {
	    @Override
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        int count = 0;
	        // Iterating through all the sub-patents and counting them
	        for (Text value : values) {
	            count++;
	        }
	        // Writing the patent id and the count of sub-patents to the output
	        context.write(key, new IntWritable(count));
	    }
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		// reads the default configuration of cluster from the configuration xml files
		Configuration conf = new Configuration();
		// Initializing the job with the default configuration of the cluster
		Job job = new Job(conf, "patent");
		// Assigning the driver class name
		job.setJarByClass(Patent.class);
		// Defining the mapper class name
		job.setMapperClass(Map.class);
		// Defining the reducer class name
		job.setReducerClass(Reduce.class);
		// Explicitly setting the out key/value type from the mapper if it is not same
		// as that of reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// Defining the output key class for the final output i.e. from reducer
		job.setOutputKeyClass(Text.class);
		// Defining the output value class for the final output i.e. from reducer
		job.setOutputValueClass(IntWritable.class);
		// Defining the output key class for the final output i.e. from reducer
		job.setOutputKeyClass(Text.class);
		// Defining the output value class for the final output i.e. from reducer
		job.setOutputValueClass(Text.class);
		// Defining input Format class which is responsible to parse the dataset into a
		// key value pair
		job.setInputFormatClass(TextInputFormat.class);
		// Defining output Format class which is responsible to parse the final
		// key-value output from MR framework to a text file into the hard disk
		job.setOutputFormatClass(TextOutputFormat.class);
		// setting the second argument as a path in a path variable
		Path outputPath = new Path(args[1]);
		// Configuring the input/output path from the filesystem into the job
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// deleting the output path automatically from hdfs so that we don't have delete
		// it explicitly
		outputPath.getFileSystem(conf).delete(outputPath);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}