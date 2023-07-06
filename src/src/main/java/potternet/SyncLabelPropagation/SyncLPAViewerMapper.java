package potternet.SyncLabelPropagation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SyncLPAViewerMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * inputFormat: "<out_node/name>\t<label>#<in_node/name>,<edge_weight>[|<in_node/name>,<edge_weight>]..."
         * outputFormat: pair with format "<<label>,<out_node/name>>"
         */
        // Parse data from text line
        String[] nameWithOtherInfo = value.toString().split("\\t");
        String[] labelWithStruct = nameWithOtherInfo[1].split("#");
        String nodeName = nameWithOtherInfo[0], label = labelWithStruct[0];
        // Send the pair to reducer
        context.write(new Text(label), new Text(nodeName));
    }
}
