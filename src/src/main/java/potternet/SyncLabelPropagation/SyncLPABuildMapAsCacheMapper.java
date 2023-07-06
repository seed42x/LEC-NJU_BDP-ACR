package potternet.SyncLabelPropagation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class SyncLPABuildMapAsCacheMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * Inputformat: "<out_node/name>\t<label>#<in_node1/name1>,<frequency>[|<in_node/name>,<frequency>]..."
         * Outputformat: "<out_node>\t<label>"
         */
        // Parse the data item form text
        String line = value.toString();
        String[] tokens = line.substring(0, line.indexOf("#")).split("\t");
        // Send the pair to reducer
        context.write(new Text(tokens[0]), new Text(tokens[1]));
    }
}
