package edu.hadoop.mr.MapReduce;

import java.io.IOException;
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

public class WordCount {

	public static class WCMapper extends Mapper<Object, Text, Text, IntWritable> {

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			final IntWritable in = new IntWritable(1);
			Text word = new Text();

			StringTokenizer st = new StringTokenizer(value.toString());
			while(st.hasMoreTokens()){
				word.set(st.nextToken());
				context.write(word, in);
			}
		}
	}

	public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text inText, Iterable<IntWritable> inInt,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			int sum= 0;
			IntWritable result = new IntWritable();
			for (IntWritable val : inInt) {
				sum = sum+val.get();				
			}
			result.set(sum);
			context.write(inText, result);
		}
	}
	
	public static class CustomPartitioner<K, V> extends org.apache.hadoop.mapreduce.Partitioner<K, V> {

		@Override
		public int getPartition(K key, V value, int numPartitions) {
			// TODO Auto-generated method stub
			return 0;
		}	
	}
	
	

	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{

		Configuration conf = new Configuration();
		Job job =Job.getInstance(conf);

		job.setJarByClass(WordCount.class);
		job.setMapperClass(WCMapper.class);
		job.setReducerClass(WCReducer.class);
		job.setCombinerClass(WCReducer.class);
		job.setPartitionerClass(CustomPartitioner.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		

		System.exit(job.waitForCompletion(true)?0:1);

	}



}
