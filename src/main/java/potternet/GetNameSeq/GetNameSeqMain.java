package potternet.GetNameSeq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.util.StringUtils;
import potternet.ConfReader.ConfReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GetNameSeqMain {
    public static void main(String[] args) throws Exception {
        /**
         * @param args: a terminal parameter, which is the path to the .yaml configuration file
         * @note: config a Hadoop MapReduce job, whose input is: a name list, some text files,
         *          output is: a name sequence store in a text file by format: "<name1>,<name2>,<name3>...\n",
         *          and each line corresponds to a sentence.
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
        String nameListPath = "name_list_path";
        ConfReader confReader = new ConfReader(otherArgs[0], Arrays.asList(
                nameListPath, inputFolderPath, outputFolderPath
        ));

        // Config Hadoop MapReduce Job
        Job job = Job.getInstance(conf, "GetNameSeq");
        job.setJarByClass(GetNameSeqMain.class);
        // Set the input folder and ignore the name list
        // Filter the name list in input folder
        List<Path> inputFiles = getInputFiles(
                FileSystem.get(conf),
                new Path(confReader.getAttr(inputFolderPath)),
                new Path(confReader.getAttr(nameListPath))
        );
        FileInputFormat.setInputPaths(job, StringUtils.join(",", inputFiles));
        FileOutputFormat.setOutputPath(job, new Path(confReader.getAttr(outputFolderPath)));

        // Load name list into cache, which will be shared by all nodes and set Mapper and Reducer
        job.addCacheFile(new Path(confReader.getAttr(nameListPath)).toUri());
        job.setMapperClass(NameSeqMapper.class);
        job.setReducerClass(NameSeqReducer.class);
        // Set output type/format
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // Exit
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    // Filter the input folder which aims to ignore the file with specific name and get input files list
    private static List<Path> getInputFiles(FileSystem fs, Path inputFolder, Path ignoreFilePath) throws IOException {
        /**
         * @param fs: HDFS filesystem object
         * @param inputFolder: the folder path which store input files
         * @param ignoreFilePath: the file we need to ignore
         * @note: ignore the ignoreFile from inputFolder
         */
        List<Path> inputFiles = new ArrayList<>();
        FileStatus[] fileStatuses = fs.listStatus(inputFolder);
        for(FileStatus status : fileStatuses) {
            Path path = status.getPath();
            if(!path.getName().equals(ignoreFilePath.getName())) {
                inputFiles.add(path);
            }
        }
        return inputFiles;
    }
}
