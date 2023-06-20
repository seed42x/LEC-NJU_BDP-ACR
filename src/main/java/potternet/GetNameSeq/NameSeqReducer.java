package potternet.GetNameSeq;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NameSeqReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    @Override
    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for(NullWritable val : values) {
            context.write(key, NullWritable.get());
        }
    }
}
