package potternet.SyncLabelPropagation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SyncLPASetUniqueLabelMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Parse the data item from text with format: "<out_node/name>\t<in_node/name1>,<frequency1>[|<in_node/name>,<frequency>]..."
        String[] tmpList = value.toString().split("\\t");
        // Initialize all node by its own name as its unique label
        String initLabelInNodeList = tmpList[0] + "#" + tmpList[1];
        // Send the pair with format: <<out_node/name>, <initLabel>#<in_node1/name,frequency>[|<in_node/name,frequency>]...>
        context.write(new Text(tmpList[0]), new Text(initLabelInNodeList));
    }
}
