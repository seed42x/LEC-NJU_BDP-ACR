package org.example.knn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class KNNCombiner
        extends Reducer<IntWritable, Text, IntWritable, Text> {
    private int K;
    protected void setup(Context context)
            throws IOException, InterruptedException {
        /**
         * Load KNN alg param: K
         */
        Configuration conf = context.getConfiguration();
        K = conf.getInt("K", 1);
    }

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        /**
         * Choose the nearest-K pairs send to reduce, aims to reduce the workload and the communication volume
         * input format: same as KNNMapper output
         * output format: input format: [<specific ID>, "<nearest-k dist>,<label>[,<nearest-k dist>,<label>]..."]
         */
        TreeMap<Double, String> tree_map = new TreeMap<Double, String>();
        for(Text val : values) {
            String[] dist_labels = val.toString().split(",");
            tree_map.put(Double.valueOf(dist_labels[0]), dist_labels[1]);
        }

        Iterator<Double> itr = tree_map.keySet().iterator();
        Map<String, Integer> map = new HashMap<String, Integer>();
        int i = 0;
        String rst = "";
        while(itr.hasNext() && i < K) {
            Double key1 = itr.next();
            rst = rst + String.valueOf(key1) + "," + tree_map.get(key1) + ",";
            i++;
        }
        context.write(key, new Text(rst));
    }
}
