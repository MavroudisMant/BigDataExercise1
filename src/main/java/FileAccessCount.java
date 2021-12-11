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
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		
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
		
			word.set(ip + "," + finalName);
			IntWritable one = new IntWritable(1);
//			context.write(word, new Text(date));
			context.write(word, one);
								
			
		}
	}
	
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		private IntWritable result = new IntWritable();
		private static final Log LOG = LogFactory.getLog(IntSumReducer.class);
		

		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
			 sum += val.get();
			}
			result.set(sum);

//			IntWritable one = new IntWritable(1);
			
			context.write(key, result);
			
//			ArrayList<Text> dates = new ArrayList<>();
//			int sum = 0;
//			for (Text val : values) {
//				int s = 0;
//				for(Text date : dates) {
//					if(date.equals(val)) {
//						s++;
//					}
//				}
//				if(s==0)
//				{
//					sum++;
//					dates.add(val);
//				}
//			}
//			
//			
//			result.set(sum);
//			LOG.info("Sum:  " + sum);
//			LOG.info("Key: " + key);
//			
//			if(sum>1)
//			{
//				context.write(key,new Text(result.toString()));
//			}
//			result.set(sum);
//			
//			IntWritable one = new IntWritable(1);
//			
//			if(result.compareTo(one) == 1)
				
		}

	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "access count");
	    job.setJarByClass(FileAccessCount.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
