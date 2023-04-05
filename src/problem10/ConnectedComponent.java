package com.cc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ConnectedComponent {
	public static class CCMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
		private final LongWritable outKey = new LongWritable();
		private final LongWritable outValue = new LongWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] nodes = value.toString().trim().split("\\s+");//got nodes of each line
			long cur_node = Long.parseLong(nodes[0]);//get value of current node
			
			//check if there is only one node in the line
			if (nodes.length == 1) {
				outKey.set(cur_node);
				outValue.set(cur_node);
				context.write(outKey, outValue);//write key-value as itself
			}
			
			//else
			for (int i = 1; i < nodes.length; i++) {
				long neighbor_node = Long.parseLong(nodes[i]);//get value of neighbor nodes
				
				//set key is the current node, value is its neighbor nodes
				outKey.set(cur_node);
				outValue.set(neighbor_node);
				context.write(outKey, outValue);
				
				//set key is its neighbor nodes, value is the current node
				outKey.set(neighbor_node);
				outValue.set(cur_node);
				context.write(outKey, outValue);
			}
		}
	}
	
	//reduce the duplicated key-value pairs
	public static class CCCombiner extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
		private final LongWritable val = new LongWritable();

		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			//using set to make sure there are no duplicated key-value pairs
			Set<Long> uniqueValues = new HashSet<Long>();
			for (LongWritable value : values) {
				uniqueValues.add(value.get());
			}
			for (Long uniqueValue : uniqueValues) {
				val.set(uniqueValue);
				System.out.print(key.get() + " " + uniqueValue + "\n");
				context.write(key, val);
			}
		}
	}
	
	public static class CCReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
		private final Map<Long, ArrayList<Long>> map = new HashMap<>();//saving current node as key and its neighbor nodes as ArrayList
		private final Map<Long, Integer> visited = new HashMap<>();//mark node is it visited

		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long cur_node = key.get();
			ArrayList<Long> list = new ArrayList<>();
			
			//iterate neighbor nodes and save them to list
			for (LongWritable value : values) {
				
				list.add(value.get());
			}
			
			//add key-value to map with key is current node and 
			//value is list of its neighbor nodes
			map.put(cur_node, list);
			
			//add key-value to visited with key is current node and
			//value is 0 as the current node is not visited
			visited.put(cur_node, 0);
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			int count = 0;//value to count connected components
			
			//iterate each node and use dfs to mark visited nodes
			for (Long node : map.keySet()) {
				if (visited.get(node) == 0) {
					dfs(node);
					count++;
				}
			}
			context.write(new LongWritable(0L), new LongWritable(count));
		}
		
		//dfs function
		private void dfs(long node) {
			visited.put(node, 1);
			ArrayList<Long> neighbor_nodes = map.get(node);
			for (int i = 0; i < neighbor_nodes.size(); i++) {
				long neighbor_node = neighbor_nodes.get(i);
				if (visited.get(neighbor_node) == 0) {
					dfs(neighbor_node);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "connected components");

		job.setJarByClass(ConnectedComponent.class);
		job.setMapperClass(CCMapper.class);
		job.setCombinerClass(CCCombiner.class);
		job.setReducerClass(CCReducer.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
