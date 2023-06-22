package potternet.ComputePageRank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public final class PRVPayload implements WritableComparable<PRVPayload> {
    private final Text name;
    private final DoubleWritable PRValue;

    public PRVPayload() {
        this.name = new Text();
        this.PRValue = new DoubleWritable();
    }

    public PRVPayload(Text name, DoubleWritable PRValue) {
        this.name = new Text(name);
        this.PRValue = new DoubleWritable(PRValue.get());
    }

    public Text getName() { return this.name; }

    public DoubleWritable getPRValue() { return this.PRValue; }

    @Override
    public int compareTo(PRVPayload another) {
        int comparePRVPayload = -1 * this.PRValue.compareTo(another.PRValue); // Descend order by PageRank Value, firstly
        if(comparePRVPayload == 0) {
            return this.name.compareTo(another.name); // Ascend order by Name, secondly
        }
        return comparePRVPayload;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.name.write(out);
        this.PRValue.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.name.readFields(in);
        this.PRValue.readFields(in);
    }
}
