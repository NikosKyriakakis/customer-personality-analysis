import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;

public class CategoryDriver extends MeanBase {
    public static class CategoryMapper extends Mapper<LongWritable, Text, Text, IntWritable> implements PersonalityAnalysisConstants {
        private final LocalDate now = LocalDate.now();

        private static final Text gold = new Text("Gold");
        private static final Text silver = new Text("Silver");
        private static final Text bronze = new Text("Bronze");
        private static final Text paper = new Text("Paper");

        public void map (
                LongWritable key,
                Text value,
                Context context
        ) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(";");

            int id;
            double  income,
                    mntWines,
                    mntFruits,
                    mntMeat,
                    mntFish,
                    mntSweets,
                    mntGold;

            try {
                income = Double.parseDouble(tokens[INCOME]);
                mntWines = Double.parseDouble(tokens[MNT_WINES]);
                mntFruits = Double.parseDouble(tokens[MNT_FRUITS]);
                mntMeat = Double.parseDouble(tokens[MNT_MEAT_PRODUCTS]);
                mntFish = Double.parseDouble(tokens[MNT_FISH_PRODUCTS]);
                mntSweets = Double.parseDouble(tokens[MNT_SWEET_PRODUCTS]);
                mntGold = Double.parseDouble(tokens[MNT_GOLD_PRODS]);
                id = Integer.parseInt(tokens[ID]);
            } catch (NumberFormatException e) {
                System.out.println(e.getMessage());
                return;
            }

            double expenses = mntWines + mntFish + mntFruits + mntMeat + mntSweets + mntGold;

            IntWritable idWritable = new IntWritable(id);

            double meanExpenses = Double.parseDouble(context.getConfiguration().get("expenses-mean"));
            double meanIncome = Double.parseDouble(context.getConfiguration().get("income-mean"));

            String[] dateParts = tokens[DT_CUSTOMER].split("/");
            if (dateParts[2].equals("21")) {
                if (income > 69500 && expenses > 1.5 * meanExpenses) {
                    // Gold
                    context.write(gold, idWritable);
                } else if (income < meanIncome && expenses <= 0.25 * meanExpenses) {
                    // Bronze
                    context.write(bronze, idWritable);
                }
            } else {
                if (income > 69500 && expenses > 1.5 * meanExpenses) {
                    // Silver
                    context.write(silver, idWritable);
                } else if (income < meanIncome && expenses < 0.25 * meanExpenses) {
                    // Paper
                    context.write(paper, idWritable);
                }
            }
        }
    }

    public static class CategoryReducer extends Reducer<Text, IntWritable, Text, Text> {
        public void reduce (
                Text key,
                Iterable<IntWritable> values,
                Context context
        ) throws IOException, InterruptedException {
            StringBuilder line = new StringBuilder();
            String keyStr = key.toString();
            if (keyStr.equals("Bronze") || keyStr.equals("Paper")) {
                return;
            }

            ArrayList<Integer> results = new ArrayList<>();
            int intValue;

            for (IntWritable value : values) {
                intValue = Integer.parseInt(value.toString());
                results.add(intValue);
            }

            System.out.println("\n\n\n\n\nRESULTS : " + results.size() + "\n\n\n\n\n");

            Collections.sort(results);

            for (int result : results) {
                line.append(result).append(", ");
            }
            context.write(new Text("\n" + keyStr), new Text(line.toString()));
        }
    }

    public void run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "Income partial sums-counts");
        job1.setJarByClass(CategoryDriver.class);
        job1.setMapperClass(DistributiveIncomeMapper.class);
        job1.setReducerClass(DistributiveReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "Calculate income mean");
        job2.setJarByClass(CategoryDriver.class);
        job2.setMapperClass(MeanBase.MeanMapper.class);
        job2.setReducerClass(MeanBase.MeanReducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(MeanWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        job2.waitForCompletion(true);

        Job job3 = Job.getInstance(conf, "Expenses partial sums-counts");
        job3.setJarByClass(CategoryDriver.class);
        job3.setMapperClass(DistributiveExpenseMapper.class);
        job3.setReducerClass(DistributiveReducer.class);
        job3.setOutputKeyClass(IntWritable.class);
        job3.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job3, new Path(args[1]));
        FileOutputFormat.setOutputPath(job3, new Path(args[4]));
        job3.waitForCompletion(true);

        Job job4 = Job.getInstance(conf, "Calculate expenses mean");
        job4.setJarByClass(CategoryDriver.class);
        job4.setMapperClass(MeanBase.MeanMapper.class);
        job4.setReducerClass(MeanBase.MeanReducer.class);
        job4.setOutputKeyClass(IntWritable.class);
        job4.setOutputValueClass(MeanWritable.class);
        FileInputFormat.addInputPath(job4, new Path(args[4]));
        FileOutputFormat.setOutputPath(job4, new Path(args[5]));
        job4.waitForCompletion(true);
//
        setMean(conf, "../mean_income/part-r-00000", "income-mean");
        setMean(conf, "../mean_expenses/part-r-00000", "expenses-mean");
//
        Job job5 = Job.getInstance(conf, "Categorize clients");
        job5.setJarByClass(CategoryDriver.class);
        job5.setMapperClass(CategoryMapper.class);
        job5.setReducerClass(CategoryReducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job5, new Path(args[1]));
        FileOutputFormat.setOutputPath(job5, new Path(args[6]));
        job5.waitForCompletion(true);
    }
}
