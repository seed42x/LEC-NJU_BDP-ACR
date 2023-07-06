package potternet.BuildCharacterMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.Pair;

import java.io.IOException;
import java.util.ArrayList;

public class BuildMapReducer extends Reducer<Text, Text, Text, Text> {
    /**
     * input pair format: "<<out_node>, [<in_node1>#<frequency>, <in_node2>#<frequency>,...]>"
     * result format: "<out_node/name>\t<in_node/name1>,<frequency>[|<in_node/name>,<frequency>]..."
     */
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<Pair<String, Double>> recorder = new ArrayList<>();
        // Compute the summary frequency
        double sum_frequency = 0;
        for (Text val : values) {
            // Parse each edge data item
            String[] tmp = val.toString().split("#");
            String in_node = tmp[0];
            double frequency = Double.parseDouble(tmp[1]);
            recorder.add(Pair.of(in_node, frequency));
            sum_frequency += frequency;
        }
        // Normalization processing, compute each edge probability and generate final result value
        StringBuilder in_node_list = new StringBuilder();
        for(Pair<String, Double> pair : recorder) {
            double probability = pair.getSnd() / sum_frequency;
            String item = pair.getFst() + "," + probability;
            if (in_node_list.length() == 0) {
                in_node_list.append(item);
            } else {
                in_node_list.append("|").append(item);
            }
        }
        // Write result
        context.write(key, new Text(in_node_list.toString()));
    }
}
