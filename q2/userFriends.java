
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

public class userFriends {
	static String first = "";
	static String second = "";
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text userid = new Text();
		private Text list = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
             first=config.get("userA");
            second = config.get("userB");
			String[] user = value.toString().split("\t");
			if ((user.length == 2)&&(user[0].equals(first)||user[0].equals(second))) {
				
				String[] friendList = user[1].split(",");
				list.set(user[1]);
				for (int i = 0; i < friendList.length; i++) {
					String keyy;
					if (Integer.parseInt(user[0]) < Integer.parseInt(friendList[i])) {
						keyy = user[0] + "," + friendList[i];
					} else {
						keyy = friendList[i] + "," + user[0];
					}
					userid.set(keyy);
					context.write(userid, list);
				}
				

			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashSet hs = new HashSet();
			int i = 0;
			
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
			context.write(key, result);
			}
		}

	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 4) {
			System.err.println("Usage: userFriends <userA> <userB> <in> <out>");
			System.exit(2);
		}
		conf.set("userA", otherArgs[0]);
		conf.set("userB", otherArgs[1]);
		// create a job with name "MutualFriends"
		Job job = new Job(conf, "userFriends");
		job.setJarByClass(userFriends.class);
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
		FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}