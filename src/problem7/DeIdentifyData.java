package com.deiddata;

import java.io.IOException;
import java.util.List;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

public class DeIdentifyData {
	private static final Integer[] ENCRYPT_COLS = { 2, 3, 4, 5, 6, 8 };//columns need to be encrypted
	private static final String AES_ALGORITHM = "AES";//algorithm for encrypt
	private static final String SECRET_KEY = "SmallDataTeam432";//program secret key for encrypt

	public static class Map extends Mapper<Object, Text, NullWritable, Text> {
		private final List<Integer> encryptColList = Arrays.asList(ENCRYPT_COLS);//get list of encrypt columns

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] columns = value.toString().split(","); //spit each line of input data into columns
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < columns.length; i++) {
				String token = columns[i];
				if (encryptColList.contains(i + 1)) {
					token = stringToEncrypt(token);//encrypting the specified columns
				}
				if (i > 0) {
					sb.append(",");//add "," to output value
				}
				sb.append(token);//add encrypted value
			}
			context.write(NullWritable.get(), new Text(sb.toString()));
		}
	}
	
	//function to encrypt string 
	public static String stringToEncrypt(String input) {
		try {
			SecretKeySpec key = new SecretKeySpec(SECRET_KEY.getBytes(StandardCharsets.UTF_8), AES_ALGORITHM);
			Cipher cipher = Cipher.getInstance(AES_ALGORITHM);
			cipher.init(Cipher.ENCRYPT_MODE, key);
			byte[] encrypted = cipher.doFinal(input.getBytes(StandardCharsets.UTF_8));
			return Base64.getEncoder().encodeToString(encrypted);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) throws Exception {
		
		//handle exception
		if (args.length != 2) {
			System.err.println("Usage: DeidentifyData <input path> <output path>");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "deidentify data");
		
		job.setJarByClass(DeIdentifyData.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
