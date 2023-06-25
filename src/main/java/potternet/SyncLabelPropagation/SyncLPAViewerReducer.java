package potternet.SyncLabelPropagation;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SyncLPAViewerReducer extends Reducer<Text, Text, NullWritable, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        /**
         * inputFormat: <<label>,<node1/name1>[,<node/name>]...>
         * outputFormat: "<node1/name1>[|<node/name>]..."
         */
        // Parse and build name list
        StringBuilder nameList = new StringBuilder();
        for(Text val : values) {
            if(nameList.length() == 0) {
                nameList.append(val.toString());
            } else {
                nameList.append("|").append(val.toString());
            }
        }
        // Write result
        context.write(NullWritable.get(), new Text(nameList.toString()));
    }
}
