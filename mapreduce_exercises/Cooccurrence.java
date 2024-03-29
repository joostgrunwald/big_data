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

public class Cooccurrence {
    public static final String MARGINAL = "*";

    public static class StringPair implements WritableComparable<StringPair> {
        private Text first;
        private Text second;

        public StringPair() {
            this.first = new Text();
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
        public int compareTo(StringPair other) {
            int firstCompare = this.first.compareTo(other.first);
            if (firstCompare == 0) {
                boolean isMarginal = MARGINAL.equals(this.second.toString());
                boolean otherMarginal = MARGINAL.equals(other.second.toString());

                if (isMarginal && otherMarginal) {
                    return 0;
                } else if (isMarginal) {
					return -1;
                } else if (otherMarginal) {
                    return 1;
                }
				return this.second.compareTo(other.second);
            } else {
				return firstCompare;
            }
        }

        @Override
        public String toString() {
            return "(" + this.first + "," + this.second + ")";
        }

        public void set(String first, String second) {
            this.first.set(first);
            this.second.set(second);
        }

        public String getFirst() {
            return this.first.toString();
        }

        public String getSecond() {
            return this.second.toString();
        }
    }

    public static class CooccurrenceMapper
            extends Mapper<Object, Text, StringPair, IntWritable> {

        private static final StringPair PAIR = new StringPair();
        private static final IntWritable ONE = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s");

            for (int i = 0; i < tokens.length - 1; i++) {
                String first = tokens[i];
                String second = tokens[i + 1];
				
				PAIR.set(first, second);
				context.write(PAIR, ONE);
				
				PAIR.set(first, MARGINAL);
				context.write(PAIR, ONE);
				
            }
        }
    }

    public static class CooccurrenceReducer
            extends Reducer<StringPair, IntWritable, StringPair, FloatWritable> {

        private static final FloatWritable RESULT = new FloatWritable();
        private int marginalCount = 0;
		private Text wordUsed = new Text('EMPTY_TEXT');

        @Override
        public void reduce(StringPair key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
					
			//marginalTerm
            if (MARGINAL.equals(key.getSecond())) {
				if (key.getFirst().equals(wordUsed)){
					//update marginalCount
					int total = 0; 
					for (IntWritable value : values) {
						total += value.get();
					}
					marginalCount += total;	
				}
				else {
					wordUsed.set(key.getFirst());
					marginalCount = 0;
					
					int total = 0; 
					for (IntWritable value : values) {
						total += value.get();
					}
					marginalCount = total;
				}
                // TODO: compute and update marginal count
				
			//tokenTerm
            } else {
                float total = 0; // Use float instead of int, because we perform division
				for (IntWritable value : values) {
					total += value.get();
				}
				RESULT.set(total /  marginalCount);
				context.write(key, RESULT);
                // TODO: compute and output relative frequency
            }
        }
    }

    public static class PairsPartitioner
            extends Partitioner<StringPair, IntWritable> {

        @Override
        public int getPartition(StringPair key, IntWritable value, int numReduceTasks) {
            return key.getFirst().hashCode() & numReduceTasks;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Relative cooccurrence");
        job.setJarByClass(Cooccurrence.class);

        // TODO: set classes for the components:
        job.setMapperClass(CooccurrenceMapper.class);
        job.setReducerClass(CooccurrenceReducer.class);
        job.setPartitionerClass(PairsPartitioner.class);

        // TODO: set outputs of map and reduce tasks:
        job.setMapOutputKeyClass(StringPair.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(StringPair.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
