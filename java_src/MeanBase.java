import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class MeanBase {
    /**
     * This is the base class which contains the
     * functionality to collect partial sums and counts
     * and assign them to one reducer, in order to
     * calculate the algebraic mean.
     */

    public void setMean(Configuration conf, String dirPath, String varName) throws IOException {
        /**
         * This method is used to load a single
         * mean value stored from a temporary file
         */
        Path meanPath = new Path(dirPath);
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream inputStream = fs.open(meanPath);
        String fileHandle = org.apache.commons.io.IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        String []mean = fileHandle.split("\\s+");
        conf.set(varName, mean[1]);
    }

    public static class MeanMapper
            extends Mapper<LongWritable, Text, IntWritable, MeanWritable> {
        private static final IntWritable KEY = new IntWritable(1);

        /**
         * Mapper class responsible for gathering count-sum pairs in the same
         * reducer since the operation of mean calculation is algebraic
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            // Split line, skipping all consecutive spaces or tabs
            String[] tokens = value.toString().split("\\s+");
            // Parse strings to integers for (count/sum) pairs
            int count = Integer.parseInt(tokens[0]);
            int sum = Integer.parseInt(tokens[1]);
            // Associate key=1 to a new "mean" object
            context.write(KEY, new MeanWritable(count, sum));
        }
    }

    public static class MeanReducer
            extends Reducer<IntWritable, MeanWritable, Text, DoubleWritable> {
        /**
         * Reducer class which calculates a mean value from pairs of partial counts and sums
         * @param key: the file offset
         * @param values: collection of count-sum pairs
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */

        public void reduce(IntWritable key, Iterable<MeanWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0, count = 0;
            // Iterate over all values
            for (MeanWritable value : values) {
                // Extract partial sum, count
                // and update total value
                sum += value.getSum();
                count += value.getCount();
            }

            double mean = sum / count;
            context.write(new Text("Mean:"), new DoubleWritable(mean));
        }
    }
}
