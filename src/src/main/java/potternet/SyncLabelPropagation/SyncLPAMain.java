package potternet.SyncLabelPropagation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import potternet.ConfReader.ConfReader;

import javax.naming.ldap.Control;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SyncLPAMain {

    public static void main(String[] args) throws Exception {
        /**
         * @param args: a terminal parameter, which is the path to the .yaml configuration file
         * @note: config a Hadoop MapReduce job, whose input is a file which expresses an adjacency list with format:
         * "<begin_name/node>\t<end_name1/node1>,probability[|<end_name/node>,probability]..."
         * this job is a iterated task which finishes:
         * a Label Propagation Algorithm by synchronous updating without random updating order
         */
        // Read the path of .yaml configuration file
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 1) { // params: path_to_.yaml_configuration_file
            System.err.println("Usage: <path_to_.yaml_configuration_file>");
            System.exit(2);
        }

        List<Job> jobs = createControlledJobList(otherArgs[0]);
//        for(Job job : jobs) {
//            job.waitForCompletion(false);
//        }

        JobControl jobControl = new JobControl("LPA");
        ControlledJob prevJob = null;
        for(int i = 0; i < jobs.size(); i++) {
            ControlledJob controlledJob;
            if(i == 0) {
                controlledJob = new ControlledJob(jobs.get(i), null);
            } else {
                controlledJob = new ControlledJob(jobs.get(i), null);
                controlledJob.addDependingJob(prevJob);
            }
            prevJob = controlledJob;
            jobControl.addJob(controlledJob);
        }

        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.start();
        while (!jobControl.allFinished()) {
            Thread.sleep(1000);
            if(jobControl.getFailedJobList().size() != 0) {
                break;
            }
        }
        System.err.println(jobControl.getSuccessfulJobList().size());
        System.err.println(jobControl.getFailedJobList().size());
        System.exit(0);
        jobControlThread.join();
    }

    public static List<Job> createControlledJobList(String path) throws Exception {
        List<Job> jobList = new ArrayList<>();

        Configuration conf = new Configuration();
        // Parse the .yaml files into some attributions
        String inputFolderPath = "input_folder_path";
        String outputFolderPath = "output_folder_path";
        String iterationFrequency = "iteration_frequency";
        String outEdgeParamAlpha = "out_edge_param_alpha";
        String inEdgeParamBeta = "in_edge_param_beta";
        ConfReader confReader = new ConfReader(path, Arrays.asList(
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
        jobList.add(preJob);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // SyncLPABuildMapAsCache + SyncLPAIter: LPA with synchronous updating and no random order
        // Initialize the iteration initial status
        int itrDoneNum = 0;
        int itrMaxNum = Integer.parseInt(confReader.getAttr(iterationFrequency));
        while (itrDoneNum < itrMaxNum) {
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
                cacheInputFolder.append("/").append(itrFolder).append(itrDoneNum - 1);
            }
            cacheInputFolder.append("/").append(outputFile);
            cacheOutputFolder.append("/").append(itrCacheSubFolder).append(itrDoneNum);
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
            jobList.add(cacheJob);

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // SyncLPAIter: LPA Iteration
            // Config Hadoop MapReduce Job
            Job job = Job.getInstance(conf, "LPA iteration" + itrDoneNum);
            job.setJarByClass(SyncLPAMain.class);
            // Set the input & output & cache folder path
            String cacheFilePath = confReader.getAttr(outputFolderPath) +
                    "/" + itrCacheSubFolder + itrDoneNum +
                    "/" + outputFile;
            StringBuilder currInputPath = new StringBuilder();
            StringBuilder currOutputPath = new StringBuilder().append(confReader.getAttr(outputFolderPath));
            if(itrDoneNum == 0) {
                currInputPath.append(confReader.getAttr(outputFolderPath))
                        .append("/").append(pretreatmentFolder1)
                        .append("/").append(outputFile);
            } else {
                currInputPath.append(confReader.getAttr(outputFolderPath))
                        .append("/").append(itrFolder).append(itrDoneNum - 1)
                        .append("/").append(outputFile);
            }
            currOutputPath.append("/").append(itrFolder).append(itrDoneNum);
            FileInputFormat.addInputPath(job, new Path(currInputPath.toString()));
            FileOutputFormat.setOutputPath(job, new Path(currOutputPath.toString()));
            job.addCacheFile(new Path(cacheFilePath).toUri());
            // Set Parameters for LPA
            job.getConfiguration().setDouble("outEdgeParam", Double.parseDouble(confReader.getAttr(outEdgeParamAlpha)));
            job.getConfiguration().setDouble("inEdgeParam", Double.parseDouble(confReader.getAttr(inEdgeParamBeta)));
            // Set Mapper, Combiner and Reducer
            job.setMapperClass(SyncLPAIterMapper.class);
            job.setReducerClass(SyncLPAIterReducer.class);
            // Set output type/format
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            jobList.add(job);

            itrDoneNum += 1;
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // SyncLPAViewer: arrange the result of LPA, so that it's easier to see
        // Config Hadoop MapReduce Job
        Job viewerJob = Job.getInstance(conf, "LPA viewer");
        viewerJob.setJarByClass(SyncLPAMain.class);
        // Set the input & output folder path
        String viewerInputPath = confReader.getAttr(outputFolderPath) +
                "/" + itrFolder + (itrDoneNum - 1) +
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
        jobList.add(viewerJob);

        return jobList;
    }

}
