package potternet.UpperModule;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.util.GenericOptionsParser;
import potternet.BuildCharacterMap.BuildMapMain;
import potternet.ComputePageRank.PageRankMain;
import potternet.ConfReader.ConfReader;
import potternet.CountCooccurrence.CountCooccurMain;
import potternet.GetNameSeq.GetNameSeqMain;
import potternet.SyncLabelPropagation.SyncLPAMain;

import java.util.Arrays;
import java.util.List;

public class UpperModuleMain {
    public static void main(String[] args) throws Exception{
        // Read the path of .yml configuration file
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 1) { // params: path_to_.yaml_configuration_file
            System.err.println("Usage: <path_to_.yaml_configuration_file>");
            System.exit(2);
        }
        // Parse the .yaml files into some attributions
        String task1ConfPath = "task1_conf_path";
        String task2ConfPath = "task2_conf_path";
        String task3ConfPath = "task3_conf_path";
        String task4ConfPath = "task4_conf_path";
        String task5ConfPath = "task5_conf_path";

        ConfReader confReader = new ConfReader(otherArgs[0], Arrays.asList(
                task1ConfPath, task2ConfPath, task3ConfPath, task4ConfPath, task5ConfPath
        ));

        Job job1 = GetNameSeqMain.createJob(confReader.getAttr(task1ConfPath));
        Job job2 = CountCooccurMain.createJob(confReader.getAttr(task2ConfPath));
        Job job3 = BuildMapMain.createJob(confReader.getAttr(task3ConfPath));
        job1.waitForCompletion(false);
        job2.waitForCompletion(false);
        job3.waitForCompletion(false);

        List<Job> task4Jobs = SyncLPAMain.createControlledJobList(confReader.getAttr(task5ConfPath));
        for(Job task4Job : task4Jobs) {
            task4Job.waitForCompletion(false);
        }

        PageRankMain.runAllJobs(confReader.getAttr(task4ConfPath));

        System.exit(0);
    }
}
