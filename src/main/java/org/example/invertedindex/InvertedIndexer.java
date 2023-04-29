package org.example.invertedindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.net.URI;

public class InvertedIndexer {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println(otherArgs.length);
            System.err.println("Usage: <in> <out>");
            System.exit(2);
        }

        FileSystem hdfs = FileSystem.get(new URI(otherArgs[0]), conf);
        long fileCount = 0;
        FileStatus[] statuses = hdfs.listStatus(new Path(otherArgs[0]));
        for (FileStatus file : statuses) {
            if (file.isFile() && !file.getPath().getName().startsWith(".")) {
                fileCount++;
            }
        }
        conf.set("allDocs", String.valueOf(fileCount));
        hdfs.close();

        Job job = Job.getInstance(conf, "inverted index");
        job.setJarByClass(InvertedIndexer.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);         // K2
        job.setMapOutputValueClass(IIPayload.class);  // V2
        job.setOutputKeyClass(Text.class);            // K3
        job.setOutputValueClass(IIResult.class);      // V3

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
