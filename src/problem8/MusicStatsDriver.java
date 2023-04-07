package com.mr;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MusicStatsDriver {
	// Constants used in parsing LastFM data
	public static class LastFMConstants {
		public static final int USER_ID = 0;
		public static final int TRACK_ID = 1;
		public static final int IS_SHARED = 2;
		public static final int RADIO = 3;
		public static final int IS_SKIPPED = 4;
	}

	// Custom counter for invalid records
	public static enum COUNTERS {
		INVALID_RECORD_COUNT
	};

	// Mapper class for MusicStatsDriver
	public static class MusicStatsMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable ONE = new IntWritable(1);
		private Text trackId = new Text();

		// Map function to extract features from LastFM data and write output to context
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = value.toString().split("[|]");
			if (parts.length != 5) {
				// add counter for invalid records
				context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(1L);
				return;
			}

			// Extract track ID and write output to context
			trackId.set(parts[LastFMConstants.TRACK_ID]);
			context.write(new Text("unique_listeners_" + trackId), ONE);

			// If track was shared, write output to context
			if (parts[LastFMConstants.IS_SHARED].equals("1")) {
				context.write(new Text("shared_" + trackId), ONE);
			}

			// If track was listened to on radio, write output to context
			if (parts[LastFMConstants.RADIO].equals("1")) {
				context.write(new Text("listened_on_radio_" + trackId), ONE);

				// If track was skipped on radio, write output to context
				if (parts[LastFMConstants.IS_SKIPPED].equals("1")) {
					context.write(new Text("skipped_on_radio_" + trackId), ONE);
				}
			}

			// Write total listens to context
			context.write(new Text("total_listens_" + trackId), ONE);
		}
	}

	// Reducer class for MusicStatsDriver
	public static class MusicStatsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		// Reduce function to sum values and write output to context
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {

		// Create Hadoop job and set configuration
		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: MusicStatsDriver <input path> <output path>");
			System.exit(2);
		}
		Job job = new Job(conf, "Music Statistics");
		job.setJarByClass(MusicStatsDriver.class);
		job.setMapperClass(MusicStatsMapper.class);
		job.setReducerClass(MusicStatsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);

		org.apache.hadoop.mapreduce.Counters counters = job.getCounters();
		System.out.println("No. of Invalid Records :"
				+ counters.findCounter(COUNTERS.INVALID_RECORD_COUNT).getValue());
	}
}