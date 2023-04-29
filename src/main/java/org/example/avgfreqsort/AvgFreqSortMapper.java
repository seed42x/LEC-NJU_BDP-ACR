package org.example.avgfreqsort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class AvgFreqSortMapper extends Mapper<LongWritable, Text, AFSPayload, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("[,\\t]+");

        Text word = new Text(parts[0]);
        DoubleWritable avg_freq = new DoubleWritable(Double.parseDouble(parts[1]));

        context.write(new AFSPayload(word, avg_freq), NullWritable.get());
    }
}
