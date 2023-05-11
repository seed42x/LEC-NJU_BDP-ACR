package org.example.groupnation;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class GroupByNationMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        while (itr.hasMoreTokens()) {
            String[] tokens = itr.nextToken().split("\\|");
            int nationalKey = Integer.parseInt(tokens[3]);
            double accountBal = Double.parseDouble(tokens[5]);

            context.write(new IntWritable(nationalKey), new DoubleWritable(accountBal));
        }
    }
}
