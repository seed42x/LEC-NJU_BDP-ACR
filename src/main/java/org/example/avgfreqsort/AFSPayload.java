package org.example.avgfreqsort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public final class AFSPayload implements WritableComparable<AFSPayload> {
    private final Text word;
    private final DoubleWritable avg_freq;

    public AFSPayload() {
        this.word = new Text();
        this.avg_freq = new DoubleWritable();
    }

    public AFSPayload(Text word, DoubleWritable avg_freq) {
        this.word = new Text(word);
        this.avg_freq = new DoubleWritable(avg_freq.get());
    }

    public Text getWord() { return this.word; }

    public DoubleWritable getAvg_freq() { return this.avg_freq; }

    @Override
    public int compareTo(AFSPayload another) {
        int compareAvgFreq = -1 * this.avg_freq.compareTo(another.avg_freq);    // 降序排序
        if (compareAvgFreq == 0) {
            return this.word.compareTo(another.word);
        }
        return compareAvgFreq;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        word.write(out);
        avg_freq.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word.readFields(in);
        avg_freq.readFields(in);
    }

}
