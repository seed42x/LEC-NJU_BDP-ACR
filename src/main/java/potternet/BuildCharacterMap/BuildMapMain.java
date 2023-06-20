package potternet.BuildCharacterMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import potternet.ConfReader.ConfReader;

import java.util.Arrays;

public class BuildMapMain {
    public static void main(String[] args) throws Exception {
        /**
         * @param args: a terminal parameter, which is the path to the .yaml configuration file
         * @note: config a Hadoop MapReduce job, whose input is a list data with format: "<name1>,<name2>\t<co-occurrence times>"
         *          output is: a file which expresses an adjacency list with format:
         *              "<begin_name/node>:\t<end_name1/node1>,probability[|<end_name/node>,probability]..."
         */
        // Read the path of .yaml configuration file
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 1) { // params: path_to_.yaml_configuration_file
            System.err.println("Usage: <path_to_.yaml_configuration_file>");
            System.exit(2);
        }

        // Parse the .yaml files into some attributions
        String inputFolderPath = "input_folder_path";
        String outputFolderPath = "output_folder_path";
        ConfReader confReader = new ConfReader(otherArgs[0], Arrays.asList(
                inputFolderPath, outputFolderPath
        ));

        // Config Hadoop MapReduce Job
        Job job = Job.getInstance(conf, "Build Character Map");
        job.setJarByClass(BuildMapMain.class);

        // Set the input & output folder path
        FileInputFormat.addInputPath(job, new Path(confReader.getAttr(inputFolderPath)));
        FileOutputFormat.setOutputPath(job, new Path(confReader.getAttr(outputFolderPath)));

        // Set Mapper, Combiner and Reducer
        job.setMapperClass(BuildMapMapper.class);
        job.setReducerClass(BuildMapReducer.class);

        // Set output type/format
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Exit
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
