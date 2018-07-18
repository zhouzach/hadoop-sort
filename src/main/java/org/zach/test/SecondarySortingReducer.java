package org.zach.test;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SecondarySortingReducer extends
        Reducer<DateTemperaturePair, Text, DateTemperaturePair, Text> {

    @Override
    protected void reduce(DateTemperaturePair key,
                          Iterable<Text> values, Context context) throws IOException,
            InterruptedException {
        StringBuilder sortedTemperatureList = new StringBuilder();
        for (Text temperature : values) {
            sortedTemperatureList.append(temperature);
            sortedTemperatureList.append(",");
        }
        sortedTemperatureList.deleteCharAt(sortedTemperatureList.length()-1);
        context.write(key, new Text(sortedTemperatureList.toString()));
    }

}
