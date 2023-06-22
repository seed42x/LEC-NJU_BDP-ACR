package potternet.ComputePageRank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageRankGraphBuilerMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Parse the data item from text with format: "<out_node/name>\t<in_node/name1>,<frequency1>[|<in_node/name>,<frequency>]..."
        String[] tmpList = value.toString().split("\\t");
        // initialize all node with initial PR value: 1
        String initPRInNodeList = String.valueOf(1) + "#" + tmpList[1];
        // Send the pair with format: <<out_node/name>, <initPRValue>#<in_node1/name,frequency>[|<in_node1/name,frequency>]...>
        context.write(new Text(tmpList[0]), new Text(initPRInNodeList));
    }
}
