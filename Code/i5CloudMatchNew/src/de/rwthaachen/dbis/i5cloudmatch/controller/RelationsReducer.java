package de.rwthaachen.dbis.i5cloudmatch.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RelationsReducer 
        extends Reducer<Text, IntWritable, Text, Text>{

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, 
            Context context)
            throws IOException, InterruptedException {
        int sum = 0;
    
        Iterator<IntWritable> itr = values.iterator();
        while (itr.hasNext()) {
            sum += itr.next().get();
            
            List<String> articleRelEdgesOutputList=null;
            DBLPArticleXMLProcessor dblparticleprocessor= new DBLPArticleXMLProcessor();
            articleRelEdgesOutputList=dblparticleprocessor.process(key.toString());
    		for (String outputLine : articleRelEdgesOutputList) {
    			context.write(null, new Text(outputLine));	
    		}
    		
    		List<String> inproceedingRelEdgesOutputList=null;
    		//DBLPInproceedingXMLProcessor dblpinproceedingprocessor = new DBLPInproceedingXMLProcessor();
    		//inproceedingRelEdgesOutputList=dblpinproceedingprocessor.process(key.toString());
    		for (String outputLine : inproceedingRelEdgesOutputList) {
    			context.write(null, new Text(outputLine));	
    		}
        }
    }
}