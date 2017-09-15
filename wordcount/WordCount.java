package cloudcomp;

import java.io.IOException; 
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class WordCount {
	   
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1); 
	    private Text word = new Text();
	    
	    public void map(LongWritable key, Text value, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {
		   String line = value.toString();
		   // Using string tokenizer to tokenize the input
		   StringTokenizer tokenizer = new StringTokenizer(line);
		   while (tokenizer.hasMoreTokens()) {
			   word.set(tokenizer.nextToken());
			   // Generating the key value pair ( key, 1)
			   output.collect(word, one);
		   }
	    } 	
   }
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text key, Iterator<IntWritable> values,OutputCollector<Text, IntWritable> output, Reporter reporter) throws
			IOException {
			int sum = 0;
		    while (values.hasNext()) {
		     sum += values.next().get();
		   }
		    // Printing the final key and value pair to the output file
		   output.collect(key, new IntWritable(sum));
		 }
	}
	
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Two paramaters are required");
		}
	     JobConf conf = new JobConf(WordCount.class);
	     conf.setJobName("single-wordcount");
	     conf.setOutputKeyClass(Text.class);
	     conf.setOutputValueClass(IntWritable.class);
	     conf.setMapperClass(Map.class);
	     conf.setCombinerClass(Reduce.class);
	     conf.setReducerClass(Reduce.class);
	     conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	     // Running the job
	     JobClient.runJob(conf);
	   }
	}
