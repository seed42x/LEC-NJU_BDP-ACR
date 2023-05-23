package org.example.knn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

public class KNNMain {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length < 3 || otherArgs.length > 5) { // params: in_train_url, in_test_url, out_url, K, optimize_method
            /**
             * optimize_method:
             * 0: Classic EuclideanDist without optimization (default)
             * 1: Weighted EuclideanDist
             * 2: Gaussian Weighted Dist
             */
            System.err.println("Usage: <in_train> <in_test> <out> [<K>|(<K> <optimize_method>)]");
            System.err.println("optimize method:\n\t0: Classic EuclideanDist without optimization (default)\n\t1: Weighted EuclideanDist\n\t2: Gaussian Weighted Dist");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "KNN");
        job.setJarByClass(KNNMain.class);

        if(otherArgs.length == 4) {
            job.getConfiguration().setInt("K", Integer.parseInt(otherArgs[3]));
        } else if(otherArgs.length == 5) {
            job.getConfiguration().setInt("K", Integer.parseInt(otherArgs[3]));
            int optimize_method = Integer.parseInt(otherArgs[4]);
            if(optimize_method < 0 || optimize_method > 2) {
                System.err.println("optimize method:\n\t0: Classic EuclideanDist without optimization (default)\n\t1: Weighted EuclideanDist\n\t2: Gaussian Weighted Dist");
                System.exit(2);
            }
            job.getConfiguration().setInt("optimize_method", optimize_method);
        }

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        job.addCacheFile(new Path(otherArgs[1]).toUri());
        job.setMapperClass(KNNMapper.class);
        job.setCombinerClass(KNNCombiner.class);
        job.setReducerClass(KNNReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
