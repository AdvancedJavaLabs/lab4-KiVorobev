package org.itmo.sort;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ValueMapper extends Mapper<Object, Text, DoubleWritable, ValueData> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (fields.length == 3) {
            String category = fields[0];
            double sum = Double.parseDouble(fields[1]);
            int quantity = Integer.parseInt(fields[2]);

            context.write(new DoubleWritable(-1 * sum), new ValueData(category, quantity));
        }
    }
}
