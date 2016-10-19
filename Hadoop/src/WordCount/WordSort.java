package WordCount;

import java.io.IOException;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordSort {

	public static class SortMap extends Mapper<LongWritable, Text, LongWritable, Text> {

		private static LongWritable wordCount = new LongWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken().trim());

				if (tokenizer.hasMoreTokens()) {
					wordCount.set(Integer.valueOf(tokenizer.nextToken().trim()));
					context.write(wordCount, word);
				}

			}
		}

	}

	public static class SortReducer extends Reducer<LongWritable, Text, Text, LongWritable> {
		private Text result = new Text();

		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text val : values) {
				result.set(val.toString());
				context.write(result, key);// <k,v>互换
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordsort <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "word sort");

		job.setJarByClass(WordSort.class);

		job.setNumReduceTasks(1);
		job.setMapperClass(SortMap.class);
		job.setReducerClass(SortReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
