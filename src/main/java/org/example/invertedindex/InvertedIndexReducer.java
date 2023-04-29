package org.example.invertedindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class InvertedIndexReducer extends Reducer<Text, IIPayload, Text, IIResult> {
    @Override
    protected void reduce(Text key, Iterable<IIPayload> values, Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        int allDocs = Integer.parseInt(conf.get("allDocs"));

        Iterator<IIPayload> it = values.iterator();

        IIResult result = new IIResult();
        while (it.hasNext()) {
            IIPayload payload = it.next();
            result.addEntry(payload.getDocument(), payload.getCount());
        }
        result.finish(allDocs);
        context.write(key, result);
    }
}
