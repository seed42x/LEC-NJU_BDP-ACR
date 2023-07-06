package potternet.GetNameSeq;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class NameSeqMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private static final Map<String, String> nameDict = new HashMap<>();
    /**
     * Load name list from cache, and transform all name into lower letters
     */
    @Override
    protected void setup(Context context) throws IOException {
        // disambiguate all the possible names based on the word frequencies
        nameDict.put("Diggory", "Cedric Diggory");
        nameDict.put("Patil", "Parvati Patil");
        nameDict.put("Creevey", "Colin Creevey");
        nameDict.put("Potter", "Harry Potter");
        nameDict.put("Weasley", "Ron Weasley");
        nameDict.put("Dursley", "Dudley Dursley");

        String line;
        URI[] paths = context.getCacheFiles();
        FileSystem hdfs = FileSystem.get(paths[0], context.getConfiguration());
        BufferedReader bufReader = new BufferedReader(new InputStreamReader(hdfs.open(new Path(paths[0].getPath()))));

        Set<String> characters = new HashSet<>();
        while ((line = bufReader.readLine()) != null) {
            if (line.contains(" ")) {
                nameDict.put(line, line);
                for (String token : line.split(" ")) {
                    if (!nameDict.containsKey(token)) {
                        nameDict.put(token, line);
                    }
                }
            } else {
                characters.add(line);
            }
        }
        for (String name : characters) {
            if (!nameDict.containsKey(name)) {
                nameDict.put(name, name);
            }
        }
        bufReader.close();
    }

    /**
     * Scan text and extracting name chains from sentences, each sentence corresponds to a line data,
     * whose format is: "<name1>,<name2>,<name3>,...\n"
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        List<String> item = new ArrayList<>();

        // Split words from sentence
        String[] words = value.toString().replaceAll("[^a-zA-Z\\-]", " ").split("\\s+");
        // Use a sliding window whose length is 3 at most to check if there has person name
        int left_ptr = 0, right_ptr = Integer.min(2, words.length - 1);
        while (left_ptr < words.length) {
            // Check name in a sliding window
            int i = right_ptr;
            for (; i >= left_ptr; i--) {
                StringBuilder a_try = new StringBuilder();
                for (int j = left_ptr; j <= i; j++) {
                    a_try.append(words[j]).append(j == i ? "" : " ");
                }
                if (nameDict.containsKey(a_try.toString())) {
                    item.add(nameDict.get(a_try.toString()));
                    break;
                }
            }
            // Move the sliding window, update the left pointer and right pointer
            if (i < left_ptr) {
                left_ptr += 1;
            } else {
                left_ptr = i + 1;
            }
            right_ptr = Integer.min(left_ptr + 2, words.length - 1);
        }
        // Save the non-empty result. only when there are two person in a sentence at least
        if (item.size() > 1) {
            context.write(new Text(String.join(",", item)), NullWritable.get());
        }
    }
}
