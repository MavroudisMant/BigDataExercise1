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

public class FileAccessCount {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
		
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
			
			context.write(word, new Text(date));
								
			
		}
	}
	
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,Text>{
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			ArrayList<Text> dates = new ArrayList<>();
			int sum = 0;
			for (Text val : values) {
				int s = 0;
				for(Text date : dates) {
					if(date.equals(val)) {
						s++;
					}
				}
				if(s>0)
				{
					sum++;
				}
			}
			
			if(sum>1)
			{
				context.write(key, new Text(" "));
			}
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
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
