import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.StringTokenizer;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Inverted {
	public static class Map	extends Mapper<Object, Text, Text, Text>{
		private Text keyyy = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String lineNumber = value.toString().substring(0, value.toString().indexOf(","));
			String data =  value.toString().substring(value.toString().indexOf(",") + 1);
			StringTokenizer tokenizer = new StringTokenizer(data, ",");
			while (tokenizer.hasMoreTokens()) {
				String modifiedToken = tokenizer.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase();
				keyyy.set(modifiedToken);
				if(keyyy.toString() != "" && !keyyy.toString().isEmpty()){
					context.write(keyyy, new Text(lineNumber));
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			ArrayList<Integer> lineList = new ArrayList<Integer>();
			for (Text val: values){
				lineList.add(Integer.parseInt(val.toString()));
			}
			Collections.sort(lineList);
			context.write(key, new Text(lineList.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: Inverted Index <userData> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Inverted");
		job.setJarByClass(Inverted.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}