package org.example.grouporder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class GroupByOrder {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "groupby order");
        job.setJarByClass(GroupByOrder.class);
        job.setMapperClass(GroupByOrderMapper.class);
        job.setReducerClass(GroupByOrderReducer.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);            // K2
        job.setMapOutputValueClass(OrderPayload.class);  // V2
        job.setOutputKeyClass(OrderResultKey.class);     // K3
        job.setOutputValueClass(IntWritable.class);      // V3

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
