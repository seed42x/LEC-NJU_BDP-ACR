package potternet.SyncLabelPropagation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class SyncLPAIterMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Map<String, String> nameKeyMap = new HashMap<>();
    private double outEdgeParamAlpha;
    private double inEdgeParamBeta;

    /**
     * Load last iteration's name-label map, and params alpha & beta which is used for transforming
     * directed graph to undirected graph: u<->v, Weight_uv * alpha + Weight_vu * beta
     */
    @Override
    protected void setup(Context context) throws IOException {
        // Read the cache file and initialize name-key map
        URI[] paths = context.getCacheFiles();
        FileSystem hdfs = FileSystem.get(paths[0], context.getConfiguration());
        BufferedReader buf_reader = new BufferedReader(new InputStreamReader(hdfs.open(new Path(paths[0].getPath()))));

        String tmp;
        while ((tmp = buf_reader.readLine()) != null) {
            String name = tmp.split("\\t")[0];
            String label = tmp.split("\\t")[1];
            this.nameKeyMap.put(name, label);
        }
        // Initialize params alpha and beta from job configuration
        Configuration conf = context.getConfiguration();
        this.outEdgeParamAlpha = conf.getDouble("outEdgeParam", 1);
        this.inEdgeParamBeta = conf.getDouble("inEdgeParam", 0);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * inputFormat: "<out_node/name>\t<label>#<in_node/name>,<edge_weight>[|<in_node/name>,<edge_weight>]..."
         * outputFormat:
         * 1. for each in_node: <in_node/name>, <<out_node_label>#<param_beta * edge_weight>>
         * 2. for out_node: <out_node/name>, <<in_node_label>#<param_alpha * edge_weight>>
         * 3. keep structure: <out_node/name>, <<in_node>,<edge_weight>[|<in_node>,<edge_weight>]>
         */
        // Parse the data from text line
        String[] nodeWithOtherInfo = value.toString().split("\\t");
        String[] labelWithInNodeList = nodeWithOtherInfo[1].split("#");
        if (labelWithInNodeList.length < 2) {
            System.out.printf("%s\n", labelWithInNodeList[0]);
            System.exit(2);
        }
        String[] inNodeList = labelWithInNodeList[1].split("\\|");
        String outNodeName = nodeWithOtherInfo[0];

        // Send the pair type 1, 2 to reducer
        for(String inNodeWithEdgeWeight : inNodeList) {
            // Parse in_node and edge_weight
            String[] tmp = inNodeWithEdgeWeight.split(",");
            String inNodeName = tmp[0];
            double out2inWeight = Double.parseDouble(tmp[1]);
            // Send pair: <in_node/name>, <<out_node_label>#<param_beta * edge_weight>>
            context.write(
                new Text(inNodeName),
                    new Text(nameKeyMap.get(outNodeName) + "#" + inEdgeParamBeta * out2inWeight)
            );
            // Send pair: <out_node/name>, <<in_node_label>#<param_alpha * edge_weight>>
            context.write(
                new Text(outNodeName),
                    new Text(nameKeyMap.get(inNodeName) + "#" + outEdgeParamAlpha * out2inWeight)
            );
        }

        // Send the pair type 3 to reducer
        context.write(new Text(outNodeName), new Text(labelWithInNodeList[1]));
    }
}
