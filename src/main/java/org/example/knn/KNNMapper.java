package org.example.knn;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class KNNMapper
        extends Mapper<LongWritable, Text, IntWritable, Text> {
    static List<String> test_dataset = new ArrayList<String>();  // Local testDataset copy
    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        /**
         * Load test dataset from cache
         */
        Path[] paths = context.getLocalCacheFiles();
        BufferedReader buf_reader = new BufferedReader(new FileReader(paths[0].toUri().getPath()));
        String temp = null;
        while((temp = buf_reader.readLine()) != null) {
            test_dataset.add(temp);
        }
        buf_reader.close();
    }

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        /**
         * Use the line number as pair ID, send to combiner
         * pair format: [<ID>, "<distance>,<train item's type(iris type)>"]
         */
        // Transform train dataset item: String[] -> Double[]
        String[] train_temp = value.toString().split(",");
        Double[] train_item = new Double[train_temp.length - 1];
        for(int i = 0; i < train_temp.length - 1; i++) { // Attrs
            train_item[i] = Double.valueOf(train_temp[i]);
        }
        String label = train_temp[train_temp.length - 1]; // Iris Type

        // Transform test dataset item: String[] -> Double[]
        for(int i = 0; i < test_dataset.size(); i++) {
            String[] test_temp = test_dataset.get(i).split(",");
            Double[] test_item = new Double[test_temp.length];
            for(int j = 0; j < test_temp.length; j++) { // Attrs
                test_item[j] = Double.valueOf(test_temp[j]);
            }
            // Calculate Distance and send to next part
            context.write(new IntWritable(i), new Text(String.valueOf(KNNDistCalculator.EuclideanDist(test_item, train_item)) + "," + label));
        }
    }
}
