package potternet.ComputePageRank;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRankViewerReducer extends Reducer<PRVPayload, NullWritable, Text, Text> {
    /**
     * @note: Sort the PageRank result
     * @input: format: "<<out_node/name, PRValue>, NullWritable>"
     * @output: format: "<out_node/name, PRValue>"
     */
    @Override
    public void reduce(PRVPayload key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key.getName(), new Text(String.valueOf(key.getPRValue())));
    }
}
