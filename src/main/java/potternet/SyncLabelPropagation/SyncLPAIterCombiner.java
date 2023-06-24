package potternet.SyncLabelPropagation;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SyncLPAIterCombiner extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        /**
         * inputFormat:
         *      1. for each in_node: <in_node/name>, <<out_node_label>#<param_beta * edge_weight>>
         *      2. for out_node: <out_node/name>, <<in_node_label>#<param_alpha * edge_weight>>
         *      3. keep structure: <out_node/name>, <<in_node>,<edge_weight>[|<in_node>,<edge_weight>]>
         * @note: nearly do nothing, just do local aggregation for the pairs which has the same key
         */
        for(Text val : values) {
            context.write(key, val);
        }
    }
}
