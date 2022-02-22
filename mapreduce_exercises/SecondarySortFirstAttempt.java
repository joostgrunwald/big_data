import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;


public class SecondarySortFirstAttempt {

    public static class IntStringPair implements Writable {
        private IntWritable first;
        private Text second;

        public IntStringPair() {
            this.first = new IntWritable();
            this.second = new Text();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            this.first.write(out);
            this.second.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.first.readFields(in);
            this.second.readFields(in);
        }

        @Override
        public String toString() {
            return "(" + this.first + "," + this.second + ")";
        }

        public void set(int first, String second) {
            this.first.set(first);
            this.second.set(second);
        }

        public Integer getFirst() {
            return this.first.get();
        }

        public String getSecond() {
            return this.second.toString();
        }
    }

    public static class SecondarySortMapper
            extends Mapper<Object, Text, IntWritable, IntStringPair> {

        private IntWritable KEY = new IntWritable();
        private IntStringPair VALUE = new IntStringPair();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\n");
            for (String token : tokens) {
                String[] parts = token.split(":");
                if (parts.length >= 3) {
                    KEY.set(Integer.parseInt(parts[1]));
                    VALUE.set(Integer.parseInt(parts[0]), parts[2]);
                    context.write(KEY, VALUE);
                }
            }
        }
    }

    public static class SecondarySortReducer
            extends Reducer<IntWritable, IntStringPair, IntWritable, IntStringPair> {

        @Override
        public void reduce(IntWritable key, Iterable<IntStringPair> values, Context context)
                throws IOException, InterruptedException {
            for (IntStringPair val : values) {
                context.write(key, val);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Secondary sort (first attempt)");
        job.setJarByClass(SecondarySortFirstAttempt.class);

        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntStringPair.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
