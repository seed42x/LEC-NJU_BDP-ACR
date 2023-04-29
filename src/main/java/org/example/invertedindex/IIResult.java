package org.example.invertedindex;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IIResult implements WritableComparable<IIResult> {
    private final DoubleWritable average = new DoubleWritable();
    private final DoubleWritable idf = new DoubleWritable();
    private final List<IntWritable> counts = new ArrayList<>();
    private final List<Text> documents = new ArrayList<>();

    public IIResult() {
    }

    public void addEntry(Text doc, IntWritable count) {
        int length = documents.size();
        for (int i = 0; i != length; ++i) {
            if (doc.equals(documents.get(i))) {
                counts.get(i).set(counts.get(i).get() + count.get());
                return;
            }
        }
        counts.add(new IntWritable(count.get()));
        documents.add(new Text(doc.toString()));
    }

    public void finish(int allDocs) {
        double avgVal = counts.stream()
                .map(IntWritable::get)
                .mapToDouble(a -> a)
                .average().orElse(0);
        double idfVal = Math.log(allDocs / (documents.size() + 1.0));
        average.set(avgVal);
        idf.set(idfVal);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("%.2f", average.get()));
        builder.append(", ");
        int length = documents.size();
        for (int i = 0; i != length; ++i) {
            builder.append(documents.get(i).toString());
            builder.append(":");
            builder.append(counts.get(i).get());
            if (i != length - 1) {
                builder.append("; ");
            }
        }
        builder.append(", TF: ");
        builder.append(combineCount());
        builder.append("; IDF: ");
        builder.append(String.format("%.2f", idf.get()));
        return builder.toString();
    }

    public String combineDocuments() {
        return documents.stream()
                .map(Text::toString)
                .reduce((a, b) -> a + ";" + b)
                .orElse("");
    }

    public Integer combineCount() {
        return counts.stream()
                .map(IntWritable::get)
                .reduce(Integer::sum)
                .orElse(0);
    }

    @Override
    public int compareTo(IIResult iiResult) {
        int ratioCmp = average.compareTo(iiResult.average);
        if (ratioCmp == 0) {
            int countCmp = combineCount().compareTo(iiResult.combineCount());
            if (countCmp == 0) {
                return combineDocuments().compareTo(iiResult.combineDocuments());
            }
            return countCmp;
        }
        return ratioCmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        average.write(out);
        int length = documents.size();
        out.writeInt(length);
        for (int i = 0; i != length; ++i) {
            counts.get(i).write(out);
            documents.get(i).write(out);
        }
        idf.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        average.readFields(in);
        int length = in.readInt();
        for (int i = 0; i != length; ++i) {
            Text doc = new Text();
            IntWritable count = new IntWritable();

            count.readFields(in);
            doc.readFields(in);

            counts.add(count);
            documents.add(doc);
        }
        idf.readFields(in);
    }
}
