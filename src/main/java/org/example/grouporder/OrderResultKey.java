package org.example.grouporder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderResultKey implements WritableComparable<OrderResultKey> {
    private final IntWritable orderKey;

    private final Text orderPriority;

    public OrderResultKey() {
        orderKey = new IntWritable();
        orderPriority = new Text();
    }

    public OrderResultKey(IntWritable orderKey, Text orderPriority) {
        this.orderKey = orderKey;
        this.orderPriority = orderPriority;
    }

    public IntWritable getOrderKey() {
        return orderKey;
    }

    public Text getOrderPriority() {
        return orderPriority;
    }

    @Override
    public int compareTo(OrderResultKey orderResultKey) {
        int result = orderPriority.compareTo(orderResultKey.orderPriority);
        if (result == 0) {
            return orderKey.compareTo(orderResultKey.orderKey);
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        orderKey.write(dataOutput);
        orderPriority.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        orderKey.readFields(dataInput);
        orderPriority.readFields(dataInput);
    }

    @Override
    public String toString() {
        return orderKey + "\t" + orderPriority.toString();
    }
}
