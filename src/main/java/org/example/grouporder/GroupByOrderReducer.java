package org.example.grouporder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class GroupByOrderReducer extends Reducer<Text, OrderPayload, OrderResultKey, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<OrderPayload> values, Context context)
            throws IOException, InterruptedException {
        List<OrderPayload> list = new ArrayList<>();
        values.forEach(value -> list.add(value.duplicate()));
        Optional<Integer> result = list.stream()
                .map(OrderPayload::getShipPriority)
                .map(IntWritable::get)
                .reduce(Math::max);
        assert result.isPresent();
        int maxShipPriority = result.get();

        for (OrderPayload val : list) {
            if (val.getShipPriority().get() == maxShipPriority) {
                context.write(new OrderResultKey(val.getKeyCode(), key), val.getShipPriority());
            }
        }
    }
}
