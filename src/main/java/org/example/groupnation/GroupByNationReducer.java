package org.example.groupnation;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GroupByNationReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0;
        for (DoubleWritable val : values) {
            sum += val.get();
        }
        context.write(key, new DoubleWritable(sum));
    }
}
