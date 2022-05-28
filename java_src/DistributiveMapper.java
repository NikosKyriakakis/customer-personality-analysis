import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;

public class DistributiveMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> implements PersonalityAnalysisConstants {

    protected final Random generator = new Random();
    protected int column;

    @Override
    public void map (
            LongWritable key,
            Text value,
            Context context
    ) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(";");

        double x;
        try {
            x = Double.parseDouble(tokens[column]);
        } catch (NumberFormatException e) {
            System.out.println(e.getMessage());
            return;
        }

        final int bucket = generator.nextInt(BUCKETS);
        context.write(new IntWritable(bucket), new DoubleWritable(x));
    }
}