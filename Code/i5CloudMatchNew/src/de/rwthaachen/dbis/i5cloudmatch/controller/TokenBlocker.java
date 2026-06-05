package de.rwthaachen.dbis.i5cloudmatch.controller;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import de.rwthaachen.dbis.i5cloudmatch.model.Attribute;
import de.rwthaachen.dbis.i5cloudmatch.model.NeighborRBC;
import de.rwthaachen.dbis.i5cloudmatch.model.RelationalBlockedEntityCluster;
import de.rwthaachen.dbis.i5cloudmatch.model.RBC;
import de.rwthaachen.dbis.i5cloudmatch.model.Token;

public class TokenBlocker {
	public static class Map extends Mapper<LongWritable, Text,  Text, Text> {
		private Text word = new Text();
		RBC rbc;
		Token token;
		@Override
		protected void map(LongWritable key, Text value, Mapper.Context context)
				throws IOException, InterruptedException {
			Attribute attribute;
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				System.out.println(tokenizer.nextToken());
				rbc= new RBC();
				rbc.deserialize(value.toString());
				for (int i=0;i<rbc.attributes.size();i++) {
					attribute=rbc.attributes.get(i);
					token= new Token(attribute.value);
					System.out.println(token.value+" "+rbc.serialize());
					context.write(new Text(token.value), new Text(rbc.serialize()));
				}
		    }
	    }
	  
	}
    public static class Reduce extends Reducer<Text, Text, NullWritable, Text> {

  	  @Override
  	  protected void setup(Context context)
  			  throws IOException, InterruptedException {
  		  		context.write(NullWritable.get(), null);
  	  }

  	  @Override
  	  protected void cleanup(Context context)
  			  throws IOException, InterruptedException {
  		  		//context.write(new Text(""), null);
  	  }

  	  private Text outputKey = new Text();
  	  
  	  public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
  		  Iterator<Text> itr1=values.iterator();
  		  Iterator<Text> itr2=values.iterator();
  		  RBC rbc1= new RBC();
  		  RBC rbc2= new RBC();
  		  NeighborRBC nrbc;
  
  		  while (itr1.hasNext()) {
  			  rbc1.deserialize(itr1.next().toString());
  			  while (itr2.hasNext()) {
  				rbc2.deserialize(itr2.next().toString());
  				if (rbc1.globalID!=rbc2.globalID) {
  					nrbc= new NeighborRBC();
  					nrbc.globalDataSource=rbc2.globalDataSource;
  					nrbc.globalID=rbc2.globalID;
  					nrbc.entityType=rbc2.entityType;
  					nrbc.attributes=rbc2.attributes;
  					rbc1.blockNeighborEntityClusters.add(nrbc);
  				}
  			  }
  			context.write(NullWritable.get(), new Text(rbc1.serialize()));
  			  System.out.println("RESULTS:"+rbc1.serialize());
  		  }
  	  }

    }
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
        job.setJarByClass(TokenBlocker.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(TokenBlocker.Map.class);
        job.setReducerClass(TokenBlocker.Reduce.class);
        //job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);

        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path("tokenBlockeroutput"), true);
        
        FileInputFormat.addInputPath(job, new Path("xmlProcessoroutput/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("tokenBlockeroutput"));

        job.waitForCompletion(true);

	}
}
