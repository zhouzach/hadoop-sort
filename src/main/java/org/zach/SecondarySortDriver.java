package org.zach;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondarySortDriver {


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "secondary sort");
        job.setJarByClass(SecondarySortDriver.class);
        job.setJobName("SecondarySort");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Entry.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Entry.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReducer.class);
        job.setPartitionerClass(EntryPartitioner.class);
        job.setGroupingComparatorClass(EntryGroupingComparator.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
