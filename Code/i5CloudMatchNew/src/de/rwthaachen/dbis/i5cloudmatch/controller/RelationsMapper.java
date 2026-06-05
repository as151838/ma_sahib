package de.rwthaachen.dbis.i5cloudmatch.controller;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RelationsMapper 
        extends Mapper<LongWritable, Text, Text, IntWritable>{
    private static final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("<inproceedings.</inproceedings>");
        for (String w : words) {
            word.set(w);
            context.write(word, one);
        }
    }
}