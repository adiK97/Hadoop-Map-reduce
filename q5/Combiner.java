import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class Combiner {

  
  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{

    private Text word = new Text();
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {

      String[] name = value.toString().split("\t");
      word.set(name[0]);
      name[1].replace("[","").replace("]","");
      int count = name[1].split(",").length;
      for (int i = 0; i < count; i++) {
    	  context.write(word, one);
      }
    }
  }
  

  public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
	  
	private IntWritable res = new IntWritable(0);	
    private String keyyy="";
      
	
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	int c=0;
    	for(IntWritable value : values) {
    		c+=value.get();
    	}
    	if(c>res.get()) {
    		res.set(c);
			keyyy=key.toString();
		}
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	context.write(new Text(keyyy) , res);
    }
  }
  
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    if (args.length != 2) {
		System.err.println("Usage: Combiner <q5 Output> <out>");
		System.exit(2);
	}
    Job job = Job.getInstance(conf, "Combiner");
    job.setJarByClass(Combiner.class);
    job.setMapperClass(Map.class); 
    job.setCombinerClass(Reduce .class);
    job.setReducerClass(Reduce .class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}