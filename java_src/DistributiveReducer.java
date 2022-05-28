import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DistributiveReducer
        extends Reducer<IntWritable, DoubleWritable, IntWritable, IntWritable> {

    public void reduce (
            IntWritable key,
            Iterable<DoubleWritable> values,
            Context context
    ) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;
        for (DoubleWritable value : values) {
            sum += value.get();
            count++;
        }
        context.write(new IntWritable(count), new IntWritable(sum));
    }
}