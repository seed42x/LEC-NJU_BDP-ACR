package potternet.ComputePageRank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageRankViewerMapper extends Mapper<LongWritable, Text, PRVPayload, NullWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * @note: Sort the PageRank result
         * @input: format: "<out_node/name>\t<PRValue>#<in_node1,weight1>[|<in_node,weight>]..."
         * @output: format: "<<out_node/name, PRValue>, NullWritable>"
         */
        // Parse outNodeName and corresponding PageRank Value from data lines
        String[] tmp = value.toString().split("\\t");
        String name = tmp[0];
        double PRValue = Double.parseDouble(tmp[1].split("#")[0]);

        // Send the result to Reducer
        PRVPayload outputKey = new PRVPayload(new Text(name), new DoubleWritable(PRValue));
        context.write(outputKey, NullWritable.get());
    }
}
