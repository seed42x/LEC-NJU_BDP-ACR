package org.example.invertedindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public final class IIPayload implements WritableComparable<IIPayload> {
    private final Text document;
    private final IntWritable count;

    public IIPayload() {
        this.document = new Text();
        this.count = new IntWritable();
    }

    public IIPayload(Text doc, int value) {
        this.document = doc;
        this.count = new IntWritable(value);
    }

    public Text getDocument() {
        return document;
    }

    public IntWritable getCount() {
        return count;
    }

    @Override
    public int compareTo(IIPayload payload) {
        int compareText = document.compareTo(payload.getDocument());
        if (compareText == 0) {
            return count.compareTo(payload.getCount());
        }
        return compareText;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        document.write(out);
        count.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        document.readFields(in);
        count.readFields(in);
    }
}
