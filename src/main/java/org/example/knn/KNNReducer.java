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

public class KNNReducer
        extends Reducer<IntWritable, Text, IntWritable, Text> {
    private int K;
    private int optimize_method;
    protected void setup(Context context)
            throws IOException, InterruptedException {
        /**
         * Load KNN alg param: K
         */
        Configuration conf = context.getConfiguration();
        K = conf.getInt("K", 1);
        optimize_method = conf.getInt("optimize_method", 0);
    }

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        /**
         * Count the k nearest training set labels for each test dataset item,
         * and use these k labels for voting to estimate the iris type
         * input format: [<specific ID>, "<nearest-n dist>,<label>[,<nearest-n dist>,<label>]..."] (n >= k)
         * output format: [<ID/line number>, <estimating iris type>]
         */
        // Map each item's dist -> iris label/type
        TreeMap<Double, String> dist2label_map = new TreeMap<Double, String>();
        for(Text val : values) {
            String[] nearest_k_items = val.toString().split(",");
            for(int i = 0; i < nearest_k_items.length - 1; i = i + 2) {
                dist2label_map.put(Double.valueOf(nearest_k_items[i]), nearest_k_items[i+1]);
            }
        }

        // Select the nearest-k items from the n items chosen by combiner, and use them to vote possible iris type
        // (if not use combiner, there will be more items)
        Iterator<Double> itr = dist2label_map.keySet().iterator();
        Map<String, Double> label2vote_map = new HashMap<String, Double>();
        int i = 0;
        while(itr.hasNext() && i < K) {
            Double key1 = itr.next();
            if(label2vote_map.containsKey(dist2label_map.get(key1))) {
                double temp = label2vote_map.get(dist2label_map.get(key1));
                switch (optimize_method) {
                    case 0: // 0: Classic EuclideanDist without optimization (default)
                        label2vote_map.put(dist2label_map.get(key1), temp + 1);
                        break;
                    case 1: // 1: Weighted EuclideanDist
                        label2vote_map.put(dist2label_map.get(key1), temp + KNNDistCalculator.EuclideanWeight(key1));
                        break;
                    case 2: // 2: Gaussian Weighted Dist
                        label2vote_map.put(dist2label_map.get(key1), temp + KNNDistCalculator.GaussianWeight(key1));
                        break;
                }
            } else {
                switch (optimize_method) {
                    case 0: // 0: Classic EuclideanDist without optimization (default)
                        label2vote_map.put(dist2label_map.get(key1), 1.0);
                        break;
                    case 1: // 1: Weighted EuclideanDist
                        label2vote_map.put(dist2label_map.get(key1), KNNDistCalculator.GaussianWeight(key1));
                        break;
                    case 2: // 2: Gaussian Weighted Dist
                        label2vote_map.put(dist2label_map.get(key1), KNNDistCalculator.GaussianWeight(key1));
                        break;
                }
            }
            i++;
        }
        // Select the iris type with the highest number of votes as the KNN prediction result

        Iterator<String> itr1 = label2vote_map.keySet().iterator();
        if(!itr1.hasNext()) {
            System.err.println("Something wrong!");
            System.exit(2);
        }
        String label = itr1.next();
        Double count = label2vote_map.get(label);
        while (itr1.hasNext()) {
            String cur = itr1.next();
            if (count < label2vote_map.get(cur)) {
                label = cur;
                count = label2vote_map.get(label);
            }
        }

        // Save result
        Text rst = new Text();
        rst.set(label);
        context.write(key, rst);
    }
}
