package potternet.CountCooccurrence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import potternet.ConfReader.ConfReader;

import java.util.Arrays;

public class CountCooccurMain {
    public static void main(String[] args) throws Exception {
        /**
         * @param args: a terminal parameter, which is the path to the .yaml configuration file
         * @note: config a Hadoop MapReduce job, whose input is: a name sequence list
         *          out put is: a list of co-occurrence names pair
         */
        // Read the path of .yml configuration file
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 1) { // params: path_to_.yaml_configuration_file
            System.err.println("Usage: <path_to_.yaml_configuration_file>");
            System.exit(2);
        }

        // Parser the .yaml files into some attributions
        String inputFolderPath = "input_folder_path";
        String outputFolderPath = "output_folder_path";
        ConfReader confReader = new ConfReader(otherArgs[0], Arrays.asList(
                inputFolderPath, outputFolderPath
        ));

        // Config Hadoop MapReduce Job
        Job job = Job.getInstance(conf, "Count co-occurrence");
        job.setJarByClass(CountCooccurMain.class);

        // Set the input & output folder path
        FileInputFormat.addInputPath(job, new Path(confReader.getAttr(inputFolderPath)));
        FileOutputFormat.setOutputPath(job, new Path(confReader.getAttr(outputFolderPath)));

        // Set Mapper, Combiner and Reducer
        job.setMapperClass(CountCooccurMapper.class);
        job.setCombinerClass(CountCooccurCombiner.class);
        job.setReducerClass(CountCoocurReducer.class);

        // Set output type/format
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Exit
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
