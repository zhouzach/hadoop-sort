package org.zach;

import org.apache.hadoop.mapreduce.Partitioner;

public class EntryPartitioner extends Partitioner<Entry, Integer> {

    @Override
    public int getPartition(Entry entry, Integer integer, int numberPartitions) {
        return Math.abs((entry.getYearMonth().hashCode() % numberPartitions));
    }
}
