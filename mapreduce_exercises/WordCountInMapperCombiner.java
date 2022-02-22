import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountInMapperCombiner {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private static final IntWritable COUNT = new IntWritable();
        private static final Text WORD = new Text();

        private Map<String, Integer> countMap = new HashMap<>();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s");
            for (String token : tokens) {
                countMap.increment(token);
            }
        }

        @Override
        public void cleanup(Context context)
                throws IOException, InterruptedException {
            for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
                String key = entry.getKey();
                int value = entry.getValue();
				
                WORD.set(key);
				COUNT.set(value);
                context.write(WORD, COUNT);
            }
        }

        public void increment(Map<String, Integer> map, String key) {
            map.merge(key, 1, Integer::sum);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private static final IntWritable SUM = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            SUM.set(sum);
            context.write(key, SUM);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count (in-mapper combiner)");
        job.setJarByClass(WordCountInMapperCombiner.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
