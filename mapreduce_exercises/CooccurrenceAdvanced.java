import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CooccurrenceAdvanced {
    public static final String MARGINAL = "*";

    public static class StringPair implements WritableComparable<StringPair> {
        private Text first;
        private Text second;

        public StringPair() {
            this.first = new Text();
            this.second = new Text();
        }

        // TODO: implement the rest of the StringPair class
    }

    public static class CooccurrenceMapper
            extends Mapper<Object, Text, StringPair, IntWritable> {

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO: implement the map function
        }
    }

    public static class CooccurrenceReducer
            extends Reducer<StringPair, IntWritable, StringPair, FloatWritable> {

        @Override
        public void reduce(StringPair key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // TODO: implement the reduce function
        }
    }

    public static class PairsPartitioner
            extends Partitioner<StringPair, IntWritable> {

        @Override
        public int getPartition(StringPair key, IntWritable value, int numReduceTasks) {
            // TODO: implement the getPartition function
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Relative cooccurrence (advanced)");
        job.setJarByClass(CooccurrenceAdvanced.class);

        // TODO: configure the job correctly

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
