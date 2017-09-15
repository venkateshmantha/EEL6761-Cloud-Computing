package cloudcomp;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class DoubleWordCount {

	// Mapper Class for generating key-value pairs
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1); 
		private Text word = new Text();
		public void map(LongWritable key, Text value, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {
			// Tokenizing each line into words using StringTokenizer.
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			String prev = null;
			// Reading each word from the tokenizer
			while(tokenizer.hasMoreTokens()) {
				String curr = tokenizer.nextToken();
				if(prev != null) {
					// Combining previous word and current word to form pairs
					word.set(prev+" "+curr );
					output.collect(word, one);
				}
				prev = curr;
			}
		}
	}

	// Reducer class for combining values and printing out final key value pair to the output file   
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> values,OutputCollector<Text, IntWritable> output, Reporter reporter) throws
		IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}


	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Two paramaters are required");
		}
		JobConf conf = new JobConf(DoubleWordCount.class);
		conf.setJobName("double-wordcount");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		// Runnning the job
		JobClient.runJob(conf);
	}
}


