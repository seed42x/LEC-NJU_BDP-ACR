package org.example.grouporder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class GroupByOrderMapper extends Mapper<LongWritable, Text, Text, OrderPayload> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        while (itr.hasMoreTokens()) {
            String[] tokens = itr.nextToken().split("\\|");
            Text orderPriority = new Text(tokens[5]);

            int orderKey = Integer.parseInt(tokens[0]);
            int shipProperty = Integer.parseInt(tokens[7]);
            context.write(orderPriority,
                    new OrderPayload(new IntWritable(orderKey), new IntWritable(shipProperty)));
        }
    }
}
