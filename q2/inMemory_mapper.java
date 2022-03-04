import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class inMemory_mapper extends Configured implements Tool {
	static HashMap<String, String> userInfo;

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text list = new Text();
		private Text userid = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			Configuration config = context.getConfiguration();
			userInfo = new HashMap<String, String>();
			String pathh =config.get("userdata");
			FileSystem fs = FileSystem.get(config);
			Path path = new Path("hdfs://localhost:9000"+pathh);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line = br.readLine();
			int ageCount = 0;

			while (line != null) {
				String[] val = line.split(",");
				if (val.length == 10) {
					String vals = val[9];
					userInfo.put(val[0].trim(), vals);
				}
				line = br.readLine();
			}
			
			String[] userList = value.toString().split("\t");
			if ((userList.length == 2)) {

				String[] friendList = userList[1].split(",");
				int n = friendList.length;
				StringBuilder res=new StringBuilder("[");
				for (int i = 0; i < friendList.length; i++) {
					if(userInfo.containsKey(friendList[i]))
					{
						if(i==(friendList.length-1)) {
							res.append(userInfo.get(friendList[i]));
							if (Integer.parseInt(userInfo.get(friendList[i]).split("/")[2]) > 1995){
								ageCount += 1;
							}
						}
					
						else
						{
							res.append(userInfo.get(friendList[i]));
							res.append(",");
							if (Integer.parseInt(userInfo.get(friendList[i]).split("/")[2]) > 1995){
								ageCount += 1;
							}
						}
					}
				}
				res.append("], "+String.valueOf(ageCount));
				userid.set(userList[0]);
				list.set(res.toString());				
				context.write(userid, list);
			}

		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new inMemory_mapper(), args);
		System.exit(res);

	}

	@Override
	public int run(String[] otherArgs) throws Exception {
		Configuration conf = new Configuration();
		if (otherArgs.length != 6) {
			System.err.println("Usage: inMemory_mapper <userA> <userB> <in> <out> <userdata> <userout>");
			System.exit(2);
		}
		conf.set("userA", otherArgs[0]);
		conf.set("userB", otherArgs[1]);
		Job job = new Job(conf, "InlineArgument");
		job.setJarByClass(inMemory_mapper.class);
		job.setMapperClass(userFriends.Map.class);
		job.setReducerClass(userFriends.Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
		Path p=new Path(otherArgs[3]);
		FileOutputFormat.setOutputPath(job, p);
		int code = job.waitForCompletion(true) ? 0 : 1;

		Configuration conf1 = getConf();
		conf1.set("userdata", otherArgs[4]);
		Job job2 = new Job(conf1, "inMemory_mapper");
		job2.setJarByClass(inMemory_mapper.class);
		job2.setMapperClass(Map.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2,p);
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[5]));
		code = job2.waitForCompletion(true) ? 0 : 1;
		System.exit(code);
		return code;

	}

}
