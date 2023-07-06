package potternet.CountCooccurrence;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

public class CountCooccurCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * Use combiner as the local reducer to optimize MapReduce Job performance
     */
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
