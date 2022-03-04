import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MutualFriends{
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text user = new Text();
		private Text friendData = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//			
//			String q1[] = {"0,1","20,28193","1,29826","6222,19272","28041,28056"};
//			for(int j = 0; j< q1.length; j++) {				
				
			
			String[] userData = value.toString().split("\t");
			if ((userData.length == 2)) {
				String[] friendList = userData[1].split(",");
				friendData.set(userData[1]);
				for (int i = 0; i < friendList.length; i++) {
					String keyy;
					if (Integer.parseInt(userData[0]) < Integer.parseInt(friendList[i])) {
						keyy = userData[0] + "," + friendList[i];
					} else {
						keyy = friendList[i] + "," + userData[0];
					}
					user.set(keyy);
					context.write(user, friendData);
				}

			}
		}
	
		}
//	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String q1[] = {"0,1","20,28193","1,29826","6222,19272","28041,28056"};
//			Arrays.stream(q1).anyMatch(key::equals)
			
//			System.out.println("Key:   "+key.toString());
			if (Arrays.stream(q1).anyMatch(key.toString()::equals)) {
			HashSet hs = new HashSet();
			int i = 0;
//			System.out.println("Values in Reducer:   "+values);
			String res ="";
			for (Text value : values) {				
				String[] val = value.toString().split(",");
				for (int j = 0; j < val.length; j++) {
					if (i == 0) {
						hs.add(val[j]);
					} else {						
						if (hs.contains(val[j])) {
							res=res.concat(val[j]);
							res=res.concat(",");
							hs.remove(val[j]);
						}

					}					
				}
				i++;
			}
			
		
			if(!res.equals("")){
			result.set(res);
//			System.out.println("KEY:  "+key);
//			System.out.println("res  :  "+result);

			context.write(key, result);
			}
		}
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//		 get all args
		System.out.println("ININN");
		if (otherArgs.length != 2) {
			System.err.println("Usage: MutualFriends <in> <out>");
			System.exit(2);
		}
		// create a job with name "MutualFriends"
		Job job = new Job(conf, "MutualFriends");
		job.setJarByClass(MutualFriends.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		// job.setInputFormatClass(Text.class);
		// uncomment the following line to add the Combiner
		// job.setcombinerClass(Reduce.class);
		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
