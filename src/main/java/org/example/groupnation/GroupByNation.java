package org.example.groupnation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class GroupByNation {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "groupby nation");
        job.setJarByClass(GroupByNation.class);
        job.setMapperClass(GroupByNationMapper.class);
        job.setReducerClass(GroupByNationReducer.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);       // K2
        job.setMapOutputValueClass(DoubleWritable.class);  // V2
        job.setOutputKeyClass(IntWritable.class);          // K3
        job.setOutputValueClass(DoubleWritable.class);     // V3

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
