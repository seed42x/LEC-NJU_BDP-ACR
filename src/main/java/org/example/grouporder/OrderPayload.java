package org.example.grouporder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderPayload implements WritableComparable<OrderPayload> {
    private final IntWritable keyCode;
    private final IntWritable shipPriority;

    public OrderPayload() {
        this.keyCode = new IntWritable();
        this.shipPriority = new IntWritable();
    }

    public OrderPayload(IntWritable keyCode, IntWritable shipPriority) {
        this.keyCode = keyCode;
        this.shipPriority = shipPriority;
    }

    public OrderPayload duplicate() {
        return new OrderPayload(new IntWritable(keyCode.get()), new IntWritable(shipPriority.get()));
    }

    public IntWritable getKeyCode() {
        return keyCode;
    }

    public IntWritable getShipPriority() {
        return shipPriority;
    }

    @Override
    public int compareTo(OrderPayload orderPayload) {
        int result = shipPriority.compareTo(orderPayload.shipPriority);
        if (result == 0) {
            return keyCode.compareTo(orderPayload.keyCode);
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        keyCode.write(dataOutput);
        shipPriority.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        keyCode.readFields(dataInput);
        shipPriority.readFields(dataInput);
    }

    @Override
    public String toString() {
        return keyCode + "\t" + shipPriority.toString();
    }
}
