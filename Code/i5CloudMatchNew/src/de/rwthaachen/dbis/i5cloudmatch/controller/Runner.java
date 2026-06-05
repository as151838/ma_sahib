package de.rwthaachen.dbis.i5cloudmatch.controller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import de.rwthaachen.dbis.i5cloudmatch.controller.DBLPInproceedingXMLProcessor.XmlInputFormat;


public class Runner {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		/*Configuration conf = new Configuration();
		Job job = new Job(conf, "i5CloudMatch");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);*/
        Configuration conf = new Configuration();

        conf.set("xmlinput.start", "<inproceedings");
        conf.set("xmlinput.end", "</inproceedings>");
        Job parserJob = new Job(conf);
        parserJob.setJarByClass(DBLPInproceedingXMLProcessor.class);
        parserJob.setOutputKeyClass(NullWritable.class);
        parserJob.setOutputValueClass(Text.class);

        parserJob.setMapperClass(DBLPInproceedingXMLProcessor.Map.class);

        parserJob.setInputFormatClass(XmlInputFormat.class);
        parserJob.setOutputFormatClass(TextOutputFormat.class);

        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path("xmlProcessoroutput"), true);
        
        FileInputFormat.addInputPath(parserJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(parserJob, new Path("xmlProcessoroutput"));

        parserJob.waitForCompletion(true);


	}

}
