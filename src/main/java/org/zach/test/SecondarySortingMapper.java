package org.zach.test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecondarySortingMapper extends
        Mapper<LongWritable, Text, DateTemperaturePair, Text> {

    private DateTemperaturePair pair = new DateTemperaturePair();
    private Text value = new Text();

    @Override
    protected void map(LongWritable key, Text line, Context context)
            throws IOException, InterruptedException {
        String[] tokens = line.toString().split(",");
        // YYYY = tokens[0]
        // MM = tokens[1]
        // DD = tokens[2]
        // temperature = tokens[3]
        String yearMonth = tokens[0] + "-" + tokens[1];
        String day = tokens[2];
        int temperature = Integer.parseInt(tokens[3]);

        pair.setYearMonth(yearMonth);
        pair.setDay(day);
        pair.setTemperature(temperature);


        value.set(tokens[3]);
        context.write(pair, value);
    }
}
