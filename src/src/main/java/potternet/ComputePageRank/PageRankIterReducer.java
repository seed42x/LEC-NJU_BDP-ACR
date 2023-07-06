package potternet.ComputePageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRankIterReducer extends Reducer<Text, Text, Text, Text> {
    private double dampingFactor;
    private int allItemNum;

    /**
     * Load parameter "dampingFactor"(d) and "allItemNum"(N) for PageRankAlgorithm
     */
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        this.dampingFactor = conf.getDouble("dampingFactor", 0.85);
        this.allItemNum = conf.getInt("allItemNum", 100);
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        /**
         * @note: PageRank Algorithm implementation in Hadoop Reducer
         * @input: two types input pair, all for key node [u]
         * 1. for each out node [v]: <name of [u], <[v]'s PR component value for [u]: R(v) / N_v>>
         * 2. node [u]'s structure(u->*): <name of [u], <in_node1, weight1>[|<in_node, weight>]...>
         * @output: format: "<out_node/name>\t<PRValue>#<in_node1,weight1>[|<in_node,weight>]..."
         */
        // Calculate PageRank value with formula: R(u) = \frac{1 - d}{N} + d\sum_{v\in B_u}\frac{R(v)}{N_v}
        StringBuilder nodeStruct = new StringBuilder();
        double newPRValue = (1.0 - dampingFactor) / allItemNum; // R(u) = \frac{1-d}{N}
        double sum = 0.0; // sum = \sum_{v\in B_u}\frac{R(v)}{N_v}
        for (Text val : values) {
            try {   // Is <[u], R(v) / N_v>
                sum += Double.parseDouble(val.toString());
            } catch (NumberFormatException e) {     // Is <[u], <in_node1, weight1>[|<in_node, weight>]...>
                nodeStruct.append(val);
            }
        }
        newPRValue += dampingFactor * sum; // R(u) = \frac{1 - d}{N} + d\sum{v\in B_u}\frac{R(v)}{N_v}

        // Write the results for next iteration
        context.write(key, new Text(newPRValue + "#" + nodeStruct));
    }
}
