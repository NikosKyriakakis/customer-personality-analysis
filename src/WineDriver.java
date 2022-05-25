import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.time.LocalDate;

public class WineDriver extends MeanBase implements PersonalityAnalysisConstants {
    public static class WineMapper extends Mapper<LongWritable, Text, TupleWritable, NullWritable> {
        private final LocalDate now = LocalDate.now();

        public void map (
                LongWritable key,
                Text value,
                Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(";");

            double mean = Double.parseDouble(context.getConfiguration().get("mean"));
            double target = mean + 0.5 * mean;
            double mntWines;
            try {
                mntWines = Double.parseDouble(tokens[MNT_WINES]);
            } catch (NumberFormatException e) {
                System.out.println(e.getMessage());
                return;
            }

            if (mntWines > target) {
                int id, age;
                double income;
                try {
                    id = Integer.parseInt(tokens[ID]);
                    age = Integer.parseInt(tokens[AGE]);
                    income = Double.parseDouble(tokens[INCOME]);
                } catch (NumberFormatException e) {
                    System.out.println(e.getMessage());
                    return;
                }
                TupleWritable wt = new TupleWritable(id, age, income, mntWines, tokens[EDUCATION], tokens[MARITAL_STATUS]);
                wt.offset = 0;
                context.write(wt, NullWritable.get());
            }
        }
    }

    public static class OrderMapper extends Mapper<LongWritable, Text, IntWritable, TupleWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            final LocalDate now = LocalDate.now();
            IntWritable one = new IntWritable(1);
            String line = value.toString();
            String[] tokens = line.split(",");
            for (int i = 0; i < tokens.length; i++) {
                tokens[i] = tokens[i].trim();
            }
            double mntWines;
            try {
                int mntIdx = tokens.length - 1;
                mntWines = Double.parseDouble(tokens[mntIdx]);
            } catch (NumberFormatException e) {
                System.out.println(e.getMessage());
                return;
            }
            int id = Integer.parseInt(tokens[ID + 1]);
            int age = Integer.parseInt(tokens[AGE + 1]);
            double income = Double.parseDouble(tokens[INCOME + 1]);
            // age = now.getYear() - age;
            TupleWritable wt = new TupleWritable(id, age, income, mntWines, tokens[EDUCATION + 1], tokens[MARITAL_STATUS + 1]);
            context.write(one, wt);
        }
    }

    public static class OrderReducer extends Reducer<IntWritable, TupleWritable, TupleWritable, NullWritable> {
        public void reduce(IntWritable key, Iterable<TupleWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (TupleWritable value: values) {
                value.offset = ++count;
                context.write(value, NullWritable.get());
            }
        }
    }

    public void run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "Partial sums-counts");
        job1.setJarByClass(WineDriver.class);
        job1.setMapperClass(DistributiveWineMapper.class);
        job1.setReducerClass(DistributiveReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "Calculate mean");
        job2.setJarByClass(WineDriver.class);
        job2.setMapperClass(MeanBase.MeanMapper.class);
        job2.setReducerClass(MeanBase.MeanReducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(MeanWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        job2.waitForCompletion(true);

        setMean(conf, "../mean_output/part-r-00000", "mean");

        Job job3 = Job.getInstance(conf, "Multiple inputs");
        job3.setJarByClass(WineDriver.class);
        job3.setMapperClass(WineMapper.class);
        job3.setOutputKeyClass(TupleWritable.class);
        job3.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job3, new Path(args[1]));
        FileOutputFormat.setOutputPath(job3, new Path(args[4]));
        job3.waitForCompletion(true);

        Job job4 = Job.getInstance(conf, "Order");
        job4.setJarByClass(WineDriver.class);
        job4.setMapperClass(OrderMapper.class);
        job4.setReducerClass(OrderReducer.class);
        job4.setOutputKeyClass(IntWritable.class);
        job4.setOutputValueClass(TupleWritable.class);
        FileInputFormat.addInputPath(job4, new Path(args[4]));
        FileOutputFormat.setOutputPath(job4, new Path(args[5]));
        job4.waitForCompletion(true);
    }
}
