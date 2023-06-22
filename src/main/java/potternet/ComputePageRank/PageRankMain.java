package potternet.ComputePageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import potternet.ConfReader.ConfReader;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;

public class PageRankMain {
    public static void main(String[] args) throws Exception {
        /**
         * @param args: a terminal parameter, which is the path to the .yaml configuration file
         * @note: config a Hadoop MapReduce job, whose input is a file which expresses an adjacency list with format:
         *          "<begin_name/node>:\t<end_name1/node1>,probability[|<end_name2/node>,<probability>]..."
         *        this job is a iterated task which finishes a PageRank Computing
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
        String dampingFactor = "damping_factor";
        String iterationFrequency = "iteration_frequency";
        ConfReader confReader = new ConfReader(otherArgs[0], Arrays.asList(
                inputFolderPath, outputFolderPath, dampingFactor, iterationFrequency
        ));
        // Set subfolder to store intermediate results
        String pretreatmentFolder = "pretreatment";
        String itrFolder = "itr";
        String viewerFolder = "viewer";
        String outputFile = "part-r-00000";

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // PageRankGraphBuilder: Pretreatment, initialize each node's initial PageRank Value as 1
        // Config Hadoop MapReduce Job
        Job preJob = Job.getInstance(conf, "PageRankPretreatment-GraphBuilder");
        preJob.setJarByClass(PageRankMain.class);
        // Set input & output folder path
        FileInputFormat.addInputPath(preJob, new Path(confReader.getAttr(inputFolderPath)));
        FileOutputFormat.setOutputPath(preJob, new Path(confReader.getAttr(outputFolderPath) + "/" + pretreatmentFolder));
        // Set Mapper and Reducer
        preJob.setMapperClass(PageRankGraphBuilerMapper.class);
        preJob.setReducerClass(PageRankGraphBuilerReducer.class);
        // Set output type/format
        preJob.setMapOutputKeyClass(Text.class);
        preJob.setMapOutputValueClass(Text.class);
        preJob.setOutputKeyClass(Text.class);
        preJob.setOutputValueClass(Text.class);
        // Wait for completion
        preJob.waitForCompletion(true);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Count the number of data items, which is the param N in PageRank Algorithm: R(u)=\frac{1-d}{N} + d\sum_{v\in B_u}\frac{R(v)}{N_v}
        Path countInputPath = new Path(confReader.getAttr(outputFolderPath) + "/" + pretreatmentFolder + "/" + outputFile);
        int allItemNum = 0;
        try(
            FileSystem fs = countInputPath.getFileSystem(conf);
            InputStream is = fs.open(countInputPath);
            BufferedReader reader = new BufferedReader(new InputStreamReader(is))
        ) {
            String line;
            while((line = reader.readLine()) != null) {
                allItemNum++;
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // PageRankIter: PageRank Job with iteration
        // Initialize the iteration initial status
        int itrDoneNum = 0;
        int itrMaxNum = Integer.parseInt(confReader.getAttr(iterationFrequency));
        while(itrDoneNum < itrMaxNum) {
            // Config Hadoop MapReduce Job
            Job job = Job.getInstance(conf, "PageRankIteration" + itrDoneNum);
            job.setJarByClass(PageRankMain.class);
            // Config the global shared parameter: Damping Factor, all Item Number
            job.getConfiguration().setDouble("dampingFactor", Double.parseDouble(confReader.getAttr(dampingFactor)));
            job.getConfiguration().setInt("allItemNum", allItemNum);
            // Set the input & output folder path
            StringBuilder currInputFolder = new StringBuilder().append(confReader.getAttr(outputFolderPath));
            String currOutputFolder = confReader.getAttr(outputFolderPath) + "/" + itrFolder;
            if(itrDoneNum == 0) {
                currInputFolder.append("/").append(pretreatmentFolder);
            } else {
                currInputFolder.append("/").append(itrFolder).append(String.valueOf(itrDoneNum - 1));
            }
            currInputFolder.append("/").append(outputFile);
            FileInputFormat.addInputPath(job, new Path(currInputFolder.toString()));
            FileOutputFormat.setOutputPath(job, new Path(currOutputFolder + String.valueOf(itrDoneNum)));
            // Set Mapper and Reducer
            job.setMapperClass(PageRankIterMapper.class);
            job.setReducerClass(PageRankIterReducer.class);
            // Set output type/format
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            // Wait for completion
            job.waitForCompletion(true);

            // Update iteration value
            itrDoneNum += 1;
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // PageRankViewer: sort data by PageRank value
        // Config Hadoop MapReduce Job
        Job viewerJob = Job.getInstance(conf, "PageRankViewer");
        viewerJob.setJarByClass(PageRankMain.class);
        // Set the input & output folder path
        String viewerInputPath = confReader.getAttr(outputFolderPath) + "/" + itrFolder + String.valueOf(itrDoneNum - 1) + "/" + outputFile;
        String viewerOutputPah = confReader.getAttr(outputFolderPath) + "/" + viewerFolder;
        FileInputFormat.addInputPath(viewerJob, new Path(viewerInputPath));
        FileOutputFormat.setOutputPath(viewerJob, new Path(viewerOutputPah));
        // Set Mapper and Reducer
        viewerJob.setMapperClass(PageRankViewerMapper.class);
        viewerJob.setReducerClass(PageRankViewerReducer.class);
        // Set output type/format
        viewerJob.setMapOutputKeyClass(PRVPayload.class);
        viewerJob.setMapOutputValueClass(NullWritable.class);
        viewerJob.setOutputKeyClass(Text.class);
        viewerJob.setOutputValueClass(Text.class);
        // Exit
        System.exit(viewerJob.waitForCompletion(true) ? 0 : 1);
    }
}
