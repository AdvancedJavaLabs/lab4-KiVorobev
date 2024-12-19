package org.itmo.calc;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class SalesReducer extends Reducer<Text, SalesData, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<SalesData> values, Context context) throws IOException, InterruptedException {
        double totalRevenue = 0.0;
        int totalQuantity = 0;

        for (SalesData val : values) {
            totalRevenue += val.getRevenue();
            totalQuantity += val.getQuantity();
        }

        String output = new BigDecimal(totalRevenue).setScale(3, RoundingMode.HALF_UP) + "\t" + totalQuantity;
        context.write(key, new Text(output));
    }
}
