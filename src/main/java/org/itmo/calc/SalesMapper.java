package org.itmo.calc;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SalesMapper extends Mapper<Object, Text, Text, SalesData> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length == 5 && isNumber(fields[0])) {
            String category = fields[2];
            double price = Double.parseDouble(fields[3]);
            int quantity = Integer.parseInt(fields[4]);

            context.write(new Text(category), new SalesData(price * quantity, quantity));
        }
    }

    public boolean isNumber(String str) {
        return str.matches("-?\\d+");
    }
}