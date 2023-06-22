package potternet.ComputePageRank;

import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class PageRankIterMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * @note: PageRank Algorithm implementation in Hadoop Mapper
         * @input: format: "<out_node/name [u]>\t<init_PR_value>#<in_node1/name1, weight1>[|<in_node1/name1, weight>]..."
         * @output: two type output pair:
         *          1. for each in node [v]: <in_node/name [v], \frac{R([u])}{N_u}>
     *              2. the graph structure of out node [u]: <out_node/name [u], <in_node1/name1,weight1>[|<in_node,weight>]...>
         */

        // Parse data item form text lines
        String[] tmp1 = value.toString().split("\\t");
        String[] tmp2 = tmp1[1].split("#");
        String[] inNodeListTmp = tmp2[1].split("\\|");
        // Build corresponding data structure in memory
        String outNode = tmp1[0];
        Double curPRValue = Double.parseDouble(tmp2[0]);
        ArrayList<Pair<String, Double>> inNodeList = new ArrayList<>();
        for(String pairEdge : inNodeListTmp) {
            String[] tmp = pairEdge.split(",");
            if(tmp.length == 2) {
                inNodeList.add(new Pair<String, Double>(tmp[0], Double.parseDouble(tmp[1])));
            }
        }

        // Part01: Send PR component value for each in node
        // Because we have already done normalization for each node, so \frac{R(u)}{N_u} == R(u) * u-vEdgeWeight
        for(Pair<String, Double> edge : inNodeList) {
            context.write(new Text(edge.getKey()), new Text(String.valueOf(edge.getValue() * curPRValue)));
        }

        // Part02: Send the structure of out_node in graph
        context.write(new Text(outNode), new Text(tmp2[1]));
    }
}
