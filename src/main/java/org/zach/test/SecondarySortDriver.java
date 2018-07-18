package org.zach.test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SecondarySortDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration, "SecondarySort");
        job.setJarByClass(SecondarySortDriver.class);
        job.setJobName("SecondarySort");

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapperClass(SecondarySortingMapper.class);
        job.setReducerClass(SecondarySortingReducer.class);

        // 设置map输出key value格式
        job.setMapOutputKeyClass(DateTemperaturePair.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(DateTemperaturePartitioner.class);

        //在reduce端，分组排序，最后调用reduce函数
        job.setGroupingComparatorClass(DateTemperatureGroupingComparator.class);

        // 设置reduce输出key value格式
        job.setOutputKeyClass(DateTemperaturePair.class);
        job.setOutputValueClass(IntWritable.class);

        boolean status = job.waitForCompletion(true);
        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "!!!!!!!!!!!!!! Usage!!!!!!!!!!!!!!: SecondarySortDriver"
                            + "<input-path> <output-path>");
        }
        int returnStatus = ToolRunner.run(new SecondarySortDriver(), args);
        System.exit(returnStatus);
    }
}
