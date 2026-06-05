package de.rwthaachen.dbis.i5cloudmatch.controller;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RelationsJob {
    public static void main(String[] args) throws Exception {        

        String inputFile = "input/test.txt";
        String outputDirectory = "output";

        Job job = new Job();

        FileInputFormat.addInputPath(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputDirectory));

        job.setJarByClass(RelationsJob.class);
        job.setJobName("Relations Job");

        job.setMapperClass(RelationsMapper.class);
        job.setReducerClass(RelationsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}