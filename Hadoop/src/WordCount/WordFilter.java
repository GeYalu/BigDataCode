package WordCount;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WordFilter {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {

                int i = (int) tokenizer.nextToken().charAt(0);

                if (97 <= i && i <= 122) {

                    word.set(tokenizer.nextToken());
                    output.collect(word, one);


                }
            }
        }
    }






    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable,
            Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterator<IntWritable> values,
                           OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
            int sum = 0;
            while(values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }



/*    public static class MyPartitioner extends MapReduceBase
            implements Partitioner<Text, IntWritable> {

        @Override
        public int getPartition(Text key, IntWritable value, int reduceNum) {
            if (key.toString().length()<= 3) {
                return 1;
            } else {
                return 0;
            }
        }
    }*/

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(WordFilter.class);
        conf.setJobName("WordFilter");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        //conf.setPartitionerClass(MyPartitioner.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);
        //conf.setNumReduceTasks(2);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}