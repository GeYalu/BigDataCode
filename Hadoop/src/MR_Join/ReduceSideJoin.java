package MR_Join;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.LinkedList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceSideJoin {

	/*
	 * 1) 在map阶段可以通过文件路径判断来自users.txt还是login_logs.txt，来自users.txt的数据输出<userid,
	 * 'u#'+name>， 来自login_logs.txt的数据输出<userid,'l#'+login_time+'\t'+login_ip>；
	 * 2) 在reduce阶段将来自不同表的数据区分开，然后做笛卡尔乘积，输出结果；
	 * 
	 */

	public static final String DELIMITER = "\t"; // 字段分隔符

	static class MyMappper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			FileSplit split = (FileSplit) context.getInputSplit();
			String filePath = split.getPath().getName();
			

			// 获取记录字符串
			String line = value.toString();
			// 抛弃空记录
			if (line == null || line.trim().equals(""))
				return;

			String[] values = line.split(DELIMITER);
			// 处理user.txt数据
			if (filePath.contains("user")) {
				
				if (values.length < 1)
					return;
				context.write(new Text(values[0]), new Text("u#" + values[1] + DELIMITER + values[2]));
			}
			// 处理login_logs.txt数据
			else if (filePath.contains("action")) {
				
				if (values.length < 1)
					return;
				context.write(new Text(values[0]),
						new Text("a#" + values[1] + DELIMITER + values[2] + DELIMITER + values[3]));
			}else {
				return;
			}
		}
	}

	static class MyReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			LinkedList<String> linkU = new LinkedList<String>(); // users值
			LinkedList<String> linkL = new LinkedList<String>(); // login_logs值

			for (Text tval : values) {
				String val = tval.toString();
				if (val.startsWith("u#")) {
					linkU.add(val.substring(2));
				} else if (val.startsWith("a#")) {
					linkL.add(val.substring(2));
				}
			}

			for (String u : linkU) {
				for (String l : linkL) {
					context.write(key, new Text(u + DELIMITER + l));
				}
			}
		}
	}

	public static void main(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

		final String FILE_IN_PATH = args[0];
		final String FILE_OUT_PATH = args[1];

		JobConf conf = new JobConf(ReduceSideJoin.class);
		Job job = Job.getInstance(conf, "Reduce Join Demo");
		job.setMapperClass(MyMappper.class);
		job.setJarByClass(ReduceSideJoin.class);
		job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(4);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(FILE_IN_PATH));
		FileOutputFormat.setOutputPath(job, new Path(FILE_OUT_PATH));
		job.waitForCompletion(true);

	}

}