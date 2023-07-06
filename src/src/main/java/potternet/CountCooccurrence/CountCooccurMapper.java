package potternet.CountCooccurrence;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountCooccurMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Get name sequence and analysis name pairs
        String[] names = value.toString().split(",");
        for(String name1 : names) {
            for(String name2 : names) {
                if(!name1.equals(name2)) {
                    context.write(new Text(name1 + "," + name2), new IntWritable(1));
                }
            }
        }
    }
}
