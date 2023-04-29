package org.example.avgfreqsort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class AvgFreqReducer extends Reducer<AFSPayload, NullWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(AFSPayload word_freq, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(word_freq.getWord(), word_freq.getAvg_freq());
    }
}
