import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class EducationDriver {
    public static class EducationMapper
            extends Mapper<LongWritable, Text, Text, IntWritable>
            implements PersonalityAnalysisConstants
    {
        private static final IntWritable ONE = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Skip header
            if (key.get() == 0) {
                return;
            }
            // Cast Text to String and then split on every ';'
            String[] tokens = value.toString().split(";");
            // Get the educational level of a person
            String level = tokens[EDUCATION];
            // Write the new form of the data
            context.write(new Text(level), ONE);
        }
    }

    public static class EducationReducer
            extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public void run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        long startTime = System.nanoTime();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "education level count");
        job.setJarByClass(EducationDriver.class);
        job.setMapperClass(EducationMapper.class);
        job.setReducerClass(EducationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);

        long elapsedTime = System.nanoTime() - startTime;
        System.out.println("\n\n\n\n\n\n\n\n\n\n\n\nTotal execution time: " + elapsedTime / 1000000 + "ms\n");
    }
}
