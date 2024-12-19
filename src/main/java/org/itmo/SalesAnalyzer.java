package org.itmo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.itmo.calc.SalesData;
import org.itmo.calc.SalesMapper;
import org.itmo.calc.SalesReducer;
import org.itmo.sort.ValueData;
import org.itmo.sort.ValueMapper;
import org.itmo.sort.ValueReducer;

import java.io.IOException;

public class SalesAnalyzer {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String inputDir = "./input_dir/";
        String calculatedDir = "./calculated_dir";
        String resultDir = "./result_dir";
        int reducersCount = 10;

        long startTime = System.currentTimeMillis();
        Configuration conf = new Configuration();
//        conf.set("mapreduce.map.log.level", "OFF");

        Job calculateJob = Job.getInstance(conf, "Calculate job");
        calculateJob.setNumReduceTasks(reducersCount);
        calculateJob.setJarByClass(SalesAnalyzer.class);
        calculateJob.setMapperClass(SalesMapper.class);
        calculateJob.setReducerClass(SalesReducer.class);
        calculateJob.setMapOutputKeyClass(Text.class);
        calculateJob.setMapOutputValueClass(SalesData.class);
        calculateJob.setOutputKeyClass(Text.class);
        calculateJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(calculateJob, new Path(inputDir));
        Path calculatedOutput = new Path(calculatedDir);
        FileOutputFormat.setOutputPath(calculateJob, calculatedOutput);

        calculateJob.waitForCompletion(false);

        Job sortingJob = Job.getInstance(conf, "Sorting job");
//        sortingJob.setNumReduceTasks(reducersCount);
        sortingJob.setJarByClass(SalesAnalyzer.class);
        sortingJob.setMapperClass(ValueMapper.class);
        sortingJob.setReducerClass(ValueReducer.class);
        sortingJob.setMapOutputKeyClass(DoubleWritable.class);
        sortingJob.setMapOutputValueClass(ValueData.class);
        sortingJob.setOutputKeyClass(ValueData.class);
        sortingJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(sortingJob, calculatedOutput);
        FileOutputFormat.setOutputPath(sortingJob, new Path(resultDir));

        sortingJob.waitForCompletion(false);

        long endTime = System.currentTimeMillis();
        System.out.println("Execution time: " + (endTime - startTime) + " milliseconds.");
    }
}