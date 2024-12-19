package org.itmo.sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class ValueReducer extends Reducer<DoubleWritable, ValueData, Text, Text> {

    @Override
    protected void reduce(DoubleWritable key, Iterable<ValueData> values, Context context) throws IOException, InterruptedException {
        for (ValueData value : values) {
            Text category = new Text(value.getCategory());

            String output = new BigDecimal(-1 * key.get()).setScale(3, RoundingMode.HALF_UP) + "\t" + value.getQuantity();
            context.write(category, new Text(output));
        }
    }
}
