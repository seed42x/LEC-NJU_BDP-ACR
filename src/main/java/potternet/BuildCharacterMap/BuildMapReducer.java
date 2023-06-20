package potternet.BuildCharacterMap;

import javafx.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class BuildMapReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        /**
         * input pair format: "<<out_node>, [<in_node1>#<frequency>, <in_node2>#<frequency>,...]>"
         * result format: "<out_node/name>\t<in_node/name1>,<frequency>[ <in_node/name>,<frequency>]..."
         */
        ArrayList<Pair<String, Double>> recorder = new ArrayList<>();
        // Compute the summary frequency
        double sum_frequency = 0;
        for(Text val : values) {
            // Parse each edge data item
            String[] tmp = val.toString().split("#");
            String in_node = tmp[0];
            Double frequency = Double.parseDouble(tmp[1]);
            recorder.add(new Pair<String, Double>(in_node, frequency));
            sum_frequency += frequency;
        }
        // Normalization processing, compute each edge probability and generate final result value
        StringBuilder in_node_list = new StringBuilder();
        for(Pair<String, Double> pair : recorder) {
            Double probability = pair.getValue() / sum_frequency;
            String item = pair.getKey() + "," + String.valueOf(probability);
            if(in_node_list.length() == 0) {
                in_node_list.append(item);
            } else {
                in_node_list.append("|").append(item);
            }
        }
        // Write result
        context.write(key, new Text(in_node_list.toString()));
    }
}
