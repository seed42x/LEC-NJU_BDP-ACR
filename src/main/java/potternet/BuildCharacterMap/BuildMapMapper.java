package potternet.BuildCharacterMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BuildMapMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Parse the data item from text with format: "<name1>,<name2>\t<times>"
        String out_node = value.toString().split(",")[0];
        String[] tmp = value.toString().split(",")[1].split("\\t");
        String in_node = tmp[0];
        String frequency = tmp[1];
        // Send the pair with format: <<out_node/name>, <in_node/name # frequency> to reducer
        context.write(new Text(out_node), new Text(in_node + "#" + String.valueOf(frequency)));
    }
}
