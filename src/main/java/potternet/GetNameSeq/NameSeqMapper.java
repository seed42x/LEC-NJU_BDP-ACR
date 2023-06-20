package potternet.GetNameSeq;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class NameSeqMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    static HashSet<String> names = new HashSet<>(); // Local name look-up table
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        /**
         * Load name list from cache, and transform all name into lower letters
         */
        Path[] paths = context.getLocalCacheFiles();
        BufferedReader buf_reader = new BufferedReader(new FileReader(paths[0].toUri().getPath()));
        String tmp = null;
        while((tmp = buf_reader.readLine()) != null) {
            names.add(tmp.toLowerCase());
        }
        buf_reader.close();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * Scan text and extracting name chains from sentences, each sentence corresponds to a line data,
         * whose format is: "<name1>,<name2>,<name3>,...\n"
         */

        // Split words from sentence
        String[] words = value.toString().replaceAll("[^a-zA-Z\\-]", " ").toLowerCase().split("\\s+");
        // Use a sliding window whose length is 3 at most to check if there has person name
        int left_ptr = 0, right_ptr = Integer.min(2, words.length - 1);
        StringBuilder item = new StringBuilder();
        while(left_ptr < words.length) {
            // Check name in a sliding window
            int i = right_ptr;
            for (; i >= left_ptr; i--) {
                StringBuilder a_try = new StringBuilder();
                for (int j = left_ptr; j <= i; j++) {
                    a_try.append(words[j]).append(j == i ? "" : " ");
                }
                if (names.contains(a_try.toString())) {
                    if (item.length() == 0) {
                        item.append(a_try.toString());
                    } else {
                        item.append(",").append(a_try.toString());
                    }
                    break;
                }
            }
            // Move the sliding window, update the left pointer and right pointer
            if (i < left_ptr) {
                left_ptr += 1;
            } else {
                left_ptr = right_ptr + 1;
            }
            right_ptr = Integer.min(left_ptr + 2, words.length - 1);
        }
        // Save the non-empty result. only when there are two person in a sentence at least
        if(item.toString().indexOf(',') != -1) {
            context.write(new Text(item.toString()), NullWritable.get());
        }
    }
}
