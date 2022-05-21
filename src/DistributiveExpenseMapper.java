import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class DistributiveExpenseMapper extends DistributiveMapper {
    public DistributiveExpenseMapper() {
        super();
    }

    @Override
    public void map (
            LongWritable key,
            Text value,
            Context context
    ) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(";");

        double x;
        try {
            x = Double.parseDouble(tokens[MNT_WINES])
                    + Double.parseDouble(tokens[MNT_FRUITS])
                    + Double.parseDouble(tokens[MNT_MEAT_PRODUCTS])
                    + Double.parseDouble(tokens[MNT_FISH_PRODUCTS])
                    + Double.parseDouble(tokens[MNT_SWEET_PRODUCTS])
                    + Double.parseDouble(tokens[MNT_GOLD_PRODS]);
        } catch (NumberFormatException e) {
            return;
        }

        final int bucket = generator.nextInt(BUCKETS);
        context.write(new IntWritable(bucket), new DoubleWritable(x));
    }
}