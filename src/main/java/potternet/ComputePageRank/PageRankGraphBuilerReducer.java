package potternet.ComputePageRank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRankGraphBuilerReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        /**
         * @note: do nothing, just write the mappers' output
         * input & output pair format: "<out_node/name>\t<initPRValue>#<in_node/name1,frequency1>[|<in_node/name,frequency>]..."
         */
        for(Text val : values) {
            context.write(key, val);
        }
    }
}
