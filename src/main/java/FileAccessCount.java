import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FileAccessCount {
	public static class MapDates extends Mapper<Object, Text, Text, IntWritable>{
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split(",");
			
			String ip = line[0];
			String date = line[1];
			String file = line[6];
			String accessionNum = line[5];
			
			String finalName = file;
			
			if(file.charAt(0) == '.') {
				finalName = accessionNum + file;
			}
		
			word.set(ip + "," + finalName + "," + date);
			context.write(word, one);
								
			
		}
	}
	
	public static class ReduceDates extends Reducer<Text,IntWritable,Text,IntWritable>{
		private IntWritable result = new IntWritable();
		
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
			 sum += val.get();
			}
			result.set(sum);

			
			context.write(key, result);
				
		}
	}
	
	public static class MapFiles extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split(",");
			
			String ip = line[0];
			String file = line[1];
			
			word.set(ip + "," + file);
			
			context.write(word, one);
		}
	}
	
	
	public static class ReduceFiles extends Reducer<Text,IntWritable,Text,IntWritable>{
		private IntWritable result = new IntWritable();
		private IntWritable one = new IntWritable(1);
		
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			
			if(result.compareTo(one) == 1) {
				context.write(key, result);
			}
				
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		//First job
	    Configuration conf = new Configuration();
	    Job job1 = Job.getInstance(conf, "dates");
	    job1.setJarByClass(FileAccessCount.class);
	    job1.setMapperClass(MapDates.class);
	    job1.setCombinerClass(ReduceDates.class);
	    job1.setReducerClass(ReduceDates.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
	    job1.waitForCompletion(true);
	    
	    //Second job
	    Configuration conf2 = new Configuration();
	    Job job2 = Job.getInstance(conf2, "files");
	    job2.setJarByClass(FileAccessCount.class);
	    job2.setMapperClass(MapFiles.class);
	    job2.setCombinerClass(ReduceFiles.class);
	    job2.setReducerClass(ReduceFiles.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job2, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
	    
	    
	    
	    System.exit(job2.waitForCompletion(true) ? 0 : 1);
	  }
}




































