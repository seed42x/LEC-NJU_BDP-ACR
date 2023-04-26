package org.example.invertedindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public final class Payload implements WritableComparable<Payload> {
    private final Text document;
    private final IntWritable count;

    public Payload() {
        this.document = new Text();
        this.count = new IntWritable();
    }

    public Payload(Text doc, int value) {
        this.document = doc;
        this.count = new IntWritable(value);
    }

    public Text getDocument() {
        return document;
    }

    public IntWritable getCount() {
        return count;
    }

    public void addDocument(Text doc) {
        String result = document + ";" + doc.toString();
        document.set(result);
    }

    public void addCount(IntWritable adder) {
        count.set(count.get() + adder.get());
    }

    @Override
    public int compareTo(Payload payload) {
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

    @Override
    public String toString() {
        return String.format("[%s, %d]", document.toString(), count.get());
    }
}
