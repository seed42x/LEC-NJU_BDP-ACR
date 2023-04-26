package org.example.invertedindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class InvertedIndexReducer extends Reducer<Text, Payload, Text, Payload> {
    @Override
    protected void reduce(Text key, Iterable<Payload> values, Context context)
            throws IOException, InterruptedException {
        Iterator<Payload> it = values.iterator();

        Payload payload = null;
        if (it.hasNext()) {
            Payload value = it.next();
            payload = new Payload(value.getDocument(), value.getCount().get());
        }
        while (it.hasNext()) {
            Payload value = it.next();
            payload.addDocument(value.getDocument());
            payload.addCount(value.getCount());
        }
        context.write(key, payload);
    }
}
