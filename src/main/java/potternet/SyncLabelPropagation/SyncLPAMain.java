package potternet.SyncLabelPropagation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import potternet.ConfReader.ConfReader;

import java.util.Arrays;

public class SyncLPAMain {
    public static void main(String[] args) throws Exception {
        /**
         * @param args: a terminal parameter, which is the path to the .yaml configuration file
         * @note: config a Hadoop MapReduce job, whose input is a file which expresses an adjacency list with format:
         *          "<begin_name/node>\t<end_name1/node1>,probability[|<end_name/node>,probability]..."
         *          this job is a iterated task which finishes:
         *              a Label Propagation Algorithm by synchronous updating without random updating order
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
        String iterationFrequency = "iteration_frequency";
        String outEdgeParamAlpha = "out_edge_param_alpha";
        String inEdgeParamBeta = "in_edge_param_beta";
        ConfReader confReader = new ConfReader(otherArgs[0], Arrays.asList(
                inputFolderPath, outputFolderPath, iterationFrequency, outEdgeParamAlpha, inEdgeParamBeta
        ));
        // Set subfolder to store intermediate results
        String pretreatmentFolder1 = "pretreatment1";
        String itrFolder = "itr";
        String itrCacheSubFolder = "cache";
        String viewerFolder = "viewer";
        String outputFile = "part-r-00000";

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // SyncLPASetUniqueLabel (Pretreatment 1): initialize each node's by node's own name as unique label type
        // Config Hadoop MapReduce Job
        Job preJob = Job.getInstance(conf, "SyncLPASetUniqueLabel");
        preJob.setJarByClass(SyncLPAMain.class);
        // Set input & output folder path
        FileInputFormat.addInputPath(preJob, new Path(confReader.getAttr(inputFolderPath)));
        FileOutputFormat.setOutputPath(preJob, new Path(confReader.getAttr(outputFolderPath) + "/" + pretreatmentFolder1));
        // Set Mapper and Reducer
        preJob.setMapperClass(SyncLPASetUniqueLabelMapper.class);
        preJob.setReducerClass(SyncLPASetUniqueLabelReducer.class);
        // Set output type/format
        preJob.setMapOutputKeyClass(Text.class);
        preJob.setMapOutputValueClass(Text.class);
        preJob.setOutputKeyClass(Text.class);
        preJob.setOutputValueClass(Text.class);
        // Wait for completion
        preJob.waitForCompletion(true);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // SyncLPABuildMapAsCache + SyncLPAIter: LPA with synchronous updating and no random order
        // Initialize the iteration initial status
        int itrDoneNum = 0;
        int itrMaxNum = Integer.parseInt(confReader.getAttr(iterationFrequency));
        while(itrDoneNum < itrMaxNum) {
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // SyncLPABuildMapAsCache: Build Cache file from last iteration result file
            // Config Hadoop MapReduce Job
            Job cacheJob = Job.getInstance(conf, "IterationBuildCache" + itrDoneNum);
            cacheJob.setJarByClass(SyncLPAMain.class);
            // Set the input & output folder path
            StringBuilder cacheInputFolder = new StringBuilder().append(confReader.getAttr(outputFolderPath));
            StringBuilder cacheOutputFolder = new StringBuilder().append(confReader.getAttr(outputFolderPath));
            if(itrDoneNum == 0) {
                cacheInputFolder.append("/").append(pretreatmentFolder1);
            } else {
                cacheInputFolder.append("/").append(itrFolder).append(String.valueOf(itrDoneNum - 1));
            }
            cacheInputFolder.append("/").append(outputFile);
            cacheOutputFolder.append("/").append(itrCacheSubFolder).append(String.valueOf(itrDoneNum));
            FileInputFormat.addInputPath(cacheJob, new Path(cacheInputFolder.toString()));
            FileOutputFormat.setOutputPath(cacheJob, new Path(cacheOutputFolder.toString()));
            // Set Mapper and Reducer
            cacheJob.setMapperClass(SyncLPABuildMapAsCacheMapper.class);
            cacheJob.setReducerClass(SyncLPABuildMapAsCacheReducer.class);
            // Set output type/format
            cacheJob.setMapOutputKeyClass(Text.class);
            cacheJob.setMapOutputValueClass(Text.class);
            cacheJob.setOutputKeyClass(Text.class);
            cacheJob.setOutputValueClass(Text.class);
            // Wait for completion
            cacheJob.waitForCompletion(true);

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // SyncLPAIter: LPA Iteration
            // Config Hadoop MapReduce Job
            Job job = Job.getInstance(conf, "LPA iteration" + itrDoneNum);
            job.setJarByClass(SyncLPAMain.class);
            // Set the input & output & cache folder path
            String cacheFilePath = confReader.getAttr(outputFolderPath) +
                                    "/" + itrCacheSubFolder + String.valueOf(itrDoneNum) +
                                    "/" + outputFile;
            StringBuilder currInputPath = new StringBuilder();
            StringBuilder currOutputPath = new StringBuilder().append(confReader.getAttr(outputFolderPath));
            if(itrDoneNum == 0) {
                currInputPath.append(confReader.getAttr(outputFolderPath))
                                .append("/").append(pretreatmentFolder1)
                                .append("/").append(outputFile);
            } else {
                currInputPath.append(confReader.getAttr(outputFolderPath))
                                .append("/").append(itrFolder).append(String.valueOf(itrDoneNum - 1))
                                .append("/").append(outputFile);
            }
            currOutputPath.append("/").append(itrFolder).append(String.valueOf(itrDoneNum));
            FileInputFormat.addInputPath(job, new Path(currInputPath.toString()));
            FileOutputFormat.setOutputPath(job, new Path(currOutputPath.toString()));
            job.addCacheFile(new Path(cacheFilePath).toUri());
            // Set Parameters for LPA
            job.getConfiguration().setDouble("outEdgeParam", Double.parseDouble(confReader.getAttr(outEdgeParamAlpha)));
            job.getConfiguration().setDouble("inEdgeParam", Double.parseDouble(confReader.getAttr(inEdgeParamBeta)));
            // Set Mapper, Combiner and Reducer
            job.setMapperClass(SyncLPAIterMapper.class);
            job.setCombinerClass(SyncLPAIterCombiner.class);
            job.setReducerClass(SyncLPAIterReducer.class);
            // Set output type/format
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            // Wait for completion
            job.waitForCompletion(true);
            itrDoneNum += 1;
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // SyncLPAVviewer: arrange the result of LPA, so that it's easier to see
        // Config Hadoop MapReduce Job
        Job viewerJob = Job.getInstance(conf, "LPA viewer");
        viewerJob.setJarByClass(SyncLPAMain.class);
        // Set the input & output folder path
        String viewerInputPath = confReader.getAttr(outputFolderPath) +
                                "/" + itrFolder + String.valueOf(itrDoneNum - 1) +
                                "/" + outputFile;
        String viewerOutputPath = confReader.getAttr(outputFolderPath) + "/" + viewerFolder;
        FileInputFormat.addInputPath(viewerJob, new Path(viewerInputPath));
        FileOutputFormat.setOutputPath(viewerJob, new Path(viewerOutputPath));
        // Set Mapper and Reducer
        viewerJob.setMapperClass(SyncLPAViewerMapper.class);
        viewerJob.setReducerClass(SyncLPAViewerReducer.class);
        // Set output type/format
        viewerJob.setMapOutputKeyClass(Text.class);
        viewerJob.setMapOutputValueClass(Text.class);
        viewerJob.setOutputKeyClass(NullWritable.class);
        viewerJob.setOutputValueClass(Text.class);
        // Exit
        System.exit(viewerJob.waitForCompletion(true) ? 0 : 1);
    }
}
