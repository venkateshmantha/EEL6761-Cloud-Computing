package cloudcomp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.HashSet;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;

public class DistCache extends Configured implements Tool {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private static final Pattern PATTERN = Pattern.compile("\\s*\\b\\s*");

		HashSet<String> patternSet = new HashSet<String>();

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			String line = lineText.toString();
			Text currentWord = new Text();
			for (String word : PATTERN.split(line)) {
				if (!word.isEmpty() && patternSet.contains(word)) {
					currentWord = new Text(word);
					context.write(currentWord,one);
				}             
			}
		}

		protected void setup(@SuppressWarnings("rawtypes") Mapper.Context context)
				throws IOException {
			if (context.getInputSplit() instanceof FileSplit) {
				((FileSplit) context.getInputSplit()).getPath().toString();
			} else {
				context.getInputSplit().toString();
			}

			URI[] paths = context.getCacheFiles();
			parsePatternFile(paths[0]);
		}

		private void parsePatternFile(URI patternsURI) {
			try {
				BufferedReader bf = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
				String pattern;
				while ((pattern = bf.readLine()) != null) {
					for (String word : PATTERN.split(pattern)) {
						patternSet.add(word);
					}
				}
			} catch (IOException exp) {
				System.err.println("Exception while parsing the cache file " + StringUtils.stringifyException(exp));
			}
		}

	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			// Printing the final key value pair to the output file
			context.write(word, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new DistCache(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Three paramaters are required");
			return -1;
		}
		Job job = Job.getInstance(getConf(), "distcache");
		job.setJarByClass(this.getClass());
		// Input file
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// Output file
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// Path of the input cache file.
		job.addCacheFile(new Path(args[2]).toUri());
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

}


