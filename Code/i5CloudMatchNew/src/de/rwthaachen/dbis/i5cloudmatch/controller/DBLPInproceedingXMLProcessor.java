package de.rwthaachen.dbis.i5cloudmatch.controller;

import javax.xml.stream.XMLStreamConstants;//XMLInputFactory;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import de.rwthaachen.dbis.i5cloudmatch.controller.GlobalIDGenerator;
import de.rwthaachen.dbis.i5cloudmatch.model.Attribute;
import de.rwthaachen.dbis.i5cloudmatch.model.DBLPInproceedingXML;
import de.rwthaachen.dbis.i5cloudmatch.model.NeighborRBC;
import de.rwthaachen.dbis.i5cloudmatch.model.RelationalBlockedEntityCluster;
import de.rwthaachen.dbis.i5cloudmatch.model.RBC;

import javax.xml.stream.*;
import javax.xml.stream.events.Characters;

public class DBLPInproceedingXMLProcessor {
      public static class XmlInputFormat extends TextInputFormat {

      public static final String START_TAG_KEY = "xmlinput.start";
      public static final String END_TAG_KEY = "xmlinput.end";


      public RecordReader<LongWritable, Text> createRecordReader(
              InputSplit split, TaskAttemptContext context) {
          return new XmlRecordReader();
      }
      
      public static class XmlRecordReader extends
              RecordReader<LongWritable, Text> {
          private byte[] startTag;
          private byte[] endTag;
          private long start;
          private long end;
          private FSDataInputStream fsin;
          private DataOutputBuffer buffer = new DataOutputBuffer();

          private LongWritable key = new LongWritable();
          private Text value = new Text();
          @Override
          public void initialize(InputSplit split, TaskAttemptContext context)
                  throws IOException, InterruptedException {
              Configuration conf = context.getConfiguration();
              startTag = conf.get(START_TAG_KEY).getBytes("utf-8");
              endTag = conf.get(END_TAG_KEY).getBytes("utf-8");
              FileSplit fileSplit = (FileSplit) split;

              // open the file and seek to the start of the split
              start = fileSplit.getStart();
              end = start + fileSplit.getLength();
              Path file = fileSplit.getPath();
              FileSystem fs = file.getFileSystem(conf);
              fsin = fs.open(fileSplit.getPath());
              fsin.seek(start);

          }
          @Override
          public boolean nextKeyValue() throws IOException,
                  InterruptedException {
              if (fsin.getPos() < end) {
                  if (readUntilMatch(startTag, false)) {
                      try {
                          buffer.write(startTag);
                          if (readUntilMatch(endTag, true)) {
                              key.set(fsin.getPos());
                              value.set(buffer.getData(), 0,
                                      buffer.getLength());
                              return true;
                          }
                      } finally {
                          buffer.reset();
                      }
                  }
              }
              return false;
          }
         @Override
         public LongWritable getCurrentKey() throws IOException,
                  InterruptedException {
              return key;
          }

          @Override
          public Text getCurrentValue() throws IOException,
                  InterruptedException {
              return value;
          }
          @Override
          public void close() throws IOException {
              fsin.close();
          }
          @Override
          public float getProgress() throws IOException {
              return (fsin.getPos() - start) / (float) (end - start);
          }

          private boolean readUntilMatch(byte[] match, boolean withinBlock)
                  throws IOException {
              int i = 0;
              while (true) {
                  int b = fsin.read();
                  // end of file:
                  if (b == -1)
                      return false;
                  // save to buffer:
                  if (withinBlock)
                      buffer.write(b);
                  // check if we're matching:
                  if (b == match[i]) {
                      i++;
                      if (i >= match.length)
                          return true;
                  } else
                      i = 0;
                  // see if we've passed the stop point:
                  if (!withinBlock && i == 0 && fsin.getPos() >= end)
                      return false;
              }
          }
      }
  }


      public static class Map extends Mapper<LongWritable, Text,  NullWritable, Text> {
    	    public DBLPInproceedingXML inproceedingXML;
    	    public RBC inproceedingRbc; 
    	    public List<RBC> tempRbcs;
    	    public int level=0;

    	  @Override
    	  protected void map(LongWritable key, Text value, Mapper.Context context)
    	  throws IOException, InterruptedException {
    		  String document = value.toString();
    		  System.out.println("‘" + document + "‘");
    		  try {
    			  XMLStreamReader reader =
    					  XMLInputFactory.newInstance().createXMLStreamReader(new
    			  ByteArrayInputStream(document.getBytes()));
    			  String currentElement = "";
    			  String elementType="";
    			  while (reader.hasNext()) {
    				  int code = reader.next();
    				  switch (code) {
    				  	case XMLStreamConstants.START_ELEMENT: 
    				  		elementType="START_ELEMENT";
    				  		currentElement = reader.getLocalName();
    				  		System.out.println("START_ELEMENT:"+reader.getLocalName());
    				  		break;
    				  		
    				  	case XMLStreamConstants.END_ELEMENT:
    				  		elementType="END_ELEMENT";
    				  		currentElement = reader.getLocalName();
    				  		System.out.println("END_ELEMENT:"+reader.getLocalName());
    				  		break;
    				  		
    				  	case XMLStreamConstants.CHARACTERS: 
    				  		//START_ELEMENT
    				  		if (currentElement.equalsIgnoreCase("inproceedings") &&
    				  				elementType=="START_ELEMENT"){
    				  			level=1;
    							//Create new Inproceeding objects
    							inproceedingXML = new DBLPInproceedingXML();
    							//inproceeding= new DBLPInproceeding();
    							inproceedingRbc= new RBC();
    							//get new globalID
    							inproceedingRbc.globalID=GlobalIDGenerator.getGlobalID();
    							//assign entityType and globalDataSource
    							inproceedingRbc.globalDataSource="dblp";
    							inproceedingRbc.entityType="inproceeding";
    							//create new tempRbcs
    							tempRbcs= new ArrayList<RBC>();
    				  			System.out.println("inproceedings:"+reader.getText());
    				  			
    				  		} else if (currentElement.equalsIgnoreCase("author") &&
    				  				elementType=="START_ELEMENT" && level==1){
    				  			//create new Author object and assign name to Author object
    							RBC authorRbc= new RBC();
    							authorRbc.addAttribute("string", "name", reader.getText());
    							//get new globalID
    							authorRbc.globalID=GlobalIDGenerator.getGlobalID();
    							//assign entityType and globalDataSource to Author object;
    							authorRbc.entityType="author";
    							authorRbc.globalDataSource="dblp";
    							//add Author object to tempRbcs
    							tempRbcs.add(authorRbc);
    				  			System.out.println("author:"+reader.getText());
    				  			
    				  		} else if (currentElement.equalsIgnoreCase("title") &&
    				  				elementType=="START_ELEMENT" && level==1) {
    				  			//assign title to Inproceeding object
    							inproceedingRbc.addAttribute("String","title",reader.getText());
    				  			System.out.println("title:"+reader.getText());
    				  			
    				  		} else if (currentElement.equalsIgnoreCase("pages") &&
    				  				elementType=="START_ELEMENT" && level==1) {
    				  			//assign pages to Inproceeding object
    							inproceedingRbc.addAttribute("String","pages",reader.getText());
    				  			System.out.println("pages:"+reader.getText());
    				  			
    				  		} else if (currentElement.equalsIgnoreCase("year") &&
    				  				elementType=="START_ELEMENT" && level==1) {
    				  			inproceedingRbc.addAttribute("int","year",String.valueOf(reader.getText()));
    				  			System.out.println("year:"+reader.getText());
    				  			
    				  		} else if (currentElement.equalsIgnoreCase("booktitle") &&
    				  				elementType=="START_ELEMENT" && level==1) {
    				  			//create new Proceeding object and assign booktitle to Proceeding object
    							RBC proceedingRbc= new RBC();
    							proceedingRbc.addAttribute("String","booktitle",reader.getText());
    							//get new globalID
    							proceedingRbc.globalID=GlobalIDGenerator.getGlobalID();
    							//assign entityType and globalDataSource to Proceeding object
    							proceedingRbc.globalDataSource="dblp";
    							proceedingRbc.entityType="proceeding";
    							//add Proceeding object to tempRbcs
    							tempRbcs.add(proceedingRbc);
    				  			System.out.println("booktitle:"+reader.getText());
    				  			
    				  			// The following should be in inproceedings & END_ELEMENT
    				
    							//add Inproceeding object to tempRbcs
    							tempRbcs.add(inproceedingRbc);
    							level=0;
     							// process Relational Neighboors
    							for (int i=0;i<tempRbcs.size();i++) {
    								for (int j=0;j<tempRbcs.size();j++) {
    									if (tempRbcs.get(i).globalID!=tempRbcs.get(j).globalID) {
    										NeighborRBC nrbc= new NeighborRBC();
    										nrbc.globalID=tempRbcs.get(j).globalID;
    										nrbc.globalDataSource=tempRbcs.get(j).globalDataSource;
    										nrbc.entityType=tempRbcs.get(j).entityType;
    										nrbc.attributes=tempRbcs.get(j).attributes;
    										tempRbcs.get(i).relationalNeighborEntityClusters.add(nrbc);
    									}
    								}
    								context.write(NullWritable.get(), new Text(tempRbcs.get(i).serialize()));
    							}
    				  		
    				  		// END_ELEMENT	
    				  		} else if (currentElement.equalsIgnoreCase("inproceedings") &&
    				  				elementType=="END_ELEMENT"){
    				  			System.out.println("inproceedings:"+reader.getText());
    				  			
    				  		} else if (currentElement.equalsIgnoreCase("author") &&
    				  				elementType=="END_ELEMENT"){
    				  			System.out.println("author:"+reader.getText());
    				  			
    				  		} else if (currentElement.equalsIgnoreCase("title") &&
    				  				elementType=="END_ELEMENT") {
    				  			System.out.println("title:"+reader.getText());
    				  			
    				  		} else if (currentElement.equalsIgnoreCase("pages") &&
    				  				elementType=="END_ELEMENT") {
    				  			System.out.println("pages:"+reader.getText());
    				  			
    				  		} else if (currentElement.equalsIgnoreCase("year") &&
    				  				elementType=="END_ELEMENT") {
    				  			System.out.println("year:"+reader.getText());
    				  			
    				  		} else if (currentElement.equalsIgnoreCase("booktitle") &&
    				  				elementType=="END_ELEMENT") {
    				  			System.out.println("booktitle:"+reader.getText());
    				  		}
    				  				  		
    				  		break;
    				  }
    			  }
    			  reader.close();
       		  }
    		  catch(Exception e){
              throw new IOException(e);
              }

    	  }
      }
      

      public static void main(String[] args) throws Exception {
              Configuration conf = new Configuration();

              conf.set("xmlinput.start", "<inproceedings");
              conf.set("xmlinput.end", "</inproceedings>");
              Job job = new Job(conf);
              job.setJarByClass(DBLPInproceedingXMLProcessor.class);
              job.setOutputKeyClass(NullWritable.class);
              job.setOutputValueClass(Text.class);

              job.setMapperClass(DBLPInproceedingXMLProcessor.Map.class);

              job.setInputFormatClass(XmlInputFormat.class);
              job.setOutputFormatClass(TextOutputFormat.class);

              FileSystem fs = FileSystem.get(new Configuration());
              fs.delete(new Path("xmlProcessoroutput"), true);
              
              FileInputFormat.addInputPath(job, new Path("input/xmltestinput.txt"));
              FileOutputFormat.setOutputPath(job, new Path("xmlProcessoroutput"));

              job.waitForCompletion(true);
      }
}

