import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.hash.Hash;


public class ReducerJoin{
	static HashMap<String, String> userData;


	public static class userFriends extends Mapper<LongWritable, Text, LongWritable, Text> {
		private LongWritable user = new LongWritable();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//			System.out.print("In user friend mapper");
			String line[]=value.toString().split("\t");
			user.set(Long.parseLong(line[0]));
			if(line.length==2)
			{
				String friends=line[1];
				context.write(user, new Text("Friends\t"+friends.toString()));
			}

		}
	}

	public static class Reduce extends Reducer<LongWritable, Text, Text, Text> {


		static HashMap<String, String> userData;
		static HashMap<String, String> birthYears;
		private ArrayList<String> listA = new ArrayList<String>();

		public void setup(Context context) throws IOException{
//			System.out.print("In setup");
			Configuration config = context.getConfiguration();
			userData = new HashMap<String, String>();
			String pathh =config.get("userdata");
			FileSystem fs = FileSystem.get(config);
			Path path = new Path("hdfs://localhost:9000"+pathh);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line = br.readLine();
			while (line != null) {
				String[] val = line.split(",");
				if (val.length == 10) {
					String vals = val[9].split("/")[2];
					userData.put(val[0].trim(), vals);
				}
				line = br.readLine();
			}


		}
		public void reduce (LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//			System.out.print("In reducer");
			birthYears = new HashMap<String, String>();
			listA.clear();
			int minage = 0;

			for(Text val: values)
			{
				if((val.toString().split("\t")[0]).equals("Friends")) {
					listA.add((val.toString().split("\t")[1]));
					//					System.out.println("Friend list A = " +" "+ val.toString().split("\t")[1]);
				}
			}

			for(String A:listA)
			{
				minage = 0;
				String[] friends = A.split(",");
				for (int i = 0; i<friends.length; i++) {
					//						System.out.println("Friend = "+friends[i]+" "+userData.get(String.valueOf(friends[i])));
					int year = Integer.parseInt(userData.get(String.valueOf(friends[i])));
					if (year  > minage) {
						minage = year;
					}
				}
			}
			//			System.out.println(key.toString()+" "+String.valueOf(minage));
			context.write(new Text(key.toString()), new Text("\t"+String.valueOf(2022-minage)));

		}
	}


	public static void main(String[] args) throws Exception {
		System.out.print("here");
		Configuration conf = new Configuration();
		if (args.length != 3) {
			System.err.println("Usage: ReducerJoin <in> <userdata> <userout>");
			System.exit(2);
		}
		conf.set("userdata", args[1]);

		Job job = new Job(conf, "ReducerJoin");
		job.setJarByClass(ReducerJoin.class);
		job.setMapperClass(userFriends.class);
		job.setReducerClass(Reduce.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}





