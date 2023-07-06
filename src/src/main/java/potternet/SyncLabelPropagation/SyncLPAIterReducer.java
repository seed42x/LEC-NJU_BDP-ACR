package potternet.SyncLabelPropagation;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SyncLPAIterReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        /**
         * inputFormat: <<node_name>, <value>[,<value>]...>
         * <value> format:
         * 1. <<out_node_label>#<param_beta * edge_weight>>
         * 2. <<in_node_label>#<param_alpha * edge_weight>>
         * 3. keep structure: <<in_node>,<edge_weight>[|<in_node>,<edge_weight>]>
         * outputFormat: "<out_node/name>\t<label>#<in_node/name>,<edge_weight>[|<in_node/name>,<edge_weight>]..."
         *
         * @note: Use LPA's strategy to update labels synchronously
         */
        // Update the node's label by its neighbors, the new label is which the maximum number of neighbors belong to.
        StringBuilder structKeeper = new StringBuilder();
        Map<String, Double> labelVoteMap = new HashMap<>();
        for (Text val : values) {
            String tmp = val.toString();
            if (tmp.contains(",")) {
                structKeeper.append(tmp);
            } else {
                String label = tmp.split("#")[0];
                double vote = Double.parseDouble(tmp.split("#")[1]);
                labelVoteMap.put(label, (labelVoteMap.containsKey(label) ? labelVoteMap.get(label) + vote : vote));
            }
        }
        // Scan the Map to fine the label with the maximum vote
        String labelWithMaxVote = null;
        double maxVote = Double.MIN_VALUE;
        for(Map.Entry<String, Double> entry : labelVoteMap.entrySet()) {
            if(Math.abs(entry.getValue() - maxVote) > 1e-3) {
                labelWithMaxVote = entry.getKey();
                maxVote = entry.getValue();
            }
        }
        // Update label and write the result
        context.write(key, new Text(labelWithMaxVote + "#" + structKeeper));
    }
}
