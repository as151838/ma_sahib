package de.rwthaachen.dbis.i5cloudmatch.test;

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
import de.rwthaachen.dbis.i5cloudmatch.model.DBLPAuthor;
import de.rwthaachen.dbis.i5cloudmatch.model.DBLPInproceeding;
import de.rwthaachen.dbis.i5cloudmatch.model.DBLPInproceedingXML;
import de.rwthaachen.dbis.i5cloudmatch.model.DBLPProceeding;
import de.rwthaachen.dbis.i5cloudmatch.model.Entity;

import javax.xml.stream.*;
import javax.xml.stream.events.Characters;

public class DBLPInproceedingXMLProcessorOld {
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


      public static class Map extends Mapper<LongWritable, Text,  Text, Text> {
    	    public DBLPInproceedingXML inproceedingXML;
    	    //public DBLPInproceeding inproceeding;
    	    public RelationalBlockedEntityCluster inproceedingRbc;
    	    //public HashMap<Entity,String> tempEntities; 
    	    public HashMap<RelationalBlockedEntityCluster,String> tempRbcs;
    	    //public List<DBLPAuthor> tempAuthors;
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
    							inproceedingRbc= new RelationalBlockedEntityCluster();
    							//get new globalID
    							//inproceeding.globalID=GlobalIDGenerator.getGlobalID();
    							inproceedingRbc.globalID=GlobalIDGenerator.getGlobalID();
    							//assign entityType and globalDataSource
    							//inproceeding.globalDataSource="dblp";
    							inproceedingRbc.globalDataSource="dblp";
    							//inproceeding.entityType="inproceeding";
    							inproceedingRbc.entityType="inproceeding";
    							//Initialize tempAuthors
    							//tempAuthors=  new ArrayList<DBLPAuthor>();
    							//Initialize tempEntities
    							//tempEntities= new HashMap<Entity,String>();
    							tempRbcs= new HashMap<RelationalBlockedEntityCluster,String>();
    				  			System.out.println("inproceedings:"+reader.getText());
    				  			
    				  		} else if (currentElement.equalsIgnoreCase("author") &&
    				  				elementType=="START_ELEMENT" && level==1){
    				  			//create new Author object and assign name to Author object
    							//DBLPAuthor a= new DBLPAuthor(reader.getText());
    							RelationalBlockedEntityCluster authorRbc= 
    									new RelationalBlockedEntityCluster();
    							authorRbc.attributes.
    								add(new AttributeOld("String","name",reader.getText()));
    							//get new globalID
    							//a.globalID=GlobalIDGenerator.getGlobalID();
    							authorRbc.globalID=GlobalIDGenerator.getGlobalID();
    							//assign entityType and globalDataSource to Author object");
    							//a.entityType="author";
    							authorRbc.entityType="author";
    							//a.globalDataSource="dblp";
    							authorRbc.globalDataSource="dblp";
    							//add Author object to tempAuthors
    							//tempAuthors.add(a);
    							//add Author object to tempEntities
    							//tempEntities.put(a, "author");
    							tempRbcs.put(authorRbc, "author");
    				  			System.out.println("author:"+reader.getText());
    				  			
    				  		} else if (currentElement.equalsIgnoreCase("title") &&
    				  				elementType=="START_ELEMENT" && level==1) {
    				  			//assign title to Inproceeding object
    							//inproceeding.title=reader.getText();
    							inproceedingRbc.attributes.
    								add(new AttributeOld ("String","title",reader.getText()));
    				  			System.out.println("title:"+reader.getText());
    				  			
    				  		} else if (currentElement.equalsIgnoreCase("pages") &&
    				  				elementType=="START_ELEMENT" && level==1) {
    				  			//assign pages to Inproceeding object
    							//inproceeding.pages=reader.getText();
    							inproceedingRbc.attributes.
    								add(new AttributeOld("String","pages",reader.getText()));
    				  			System.out.println("pages:"+reader.getText());
    				  			
    				  		} else if (currentElement.equalsIgnoreCase("year") &&
    				  				elementType=="START_ELEMENT" && level==1) {
    				  			//inproceeding.year=Integer.parseInt(reader.getText());
    				  			inproceedingRbc.attributes.
    				  				add(new AttributeOld ("int","year",String.valueOf(reader.getText())));
    				  			System.out.println("year:"+reader.getText());
    				  			
    				  		} else if (currentElement.equalsIgnoreCase("booktitle") &&
    				  				elementType=="START_ELEMENT" && level==1) {
    				  			//create new Proceeding object and assign booktitle to Proceeding object
    							//DBLPProceeding p= new DBLPProceeding(reader.getText());
    							RelationalBlockedEntityCluster proceedingRbc=
    									new RelationalBlockedEntityCluster();
    							proceedingRbc.attributes.
    								add(new AttributeOld ("String","booktitle",reader.getText()));
    							//get new globalID
    							//p.globalID=GlobalIDGenerator.getGlobalID();
    							proceedingRbc.globalID=GlobalIDGenerator.getGlobalID();
    							//assign entityType and globalDataSource to Proceeding object
    							//p.globalDataSource="dblp";
    							proceedingRbc.globalDataSource="dblp";
    							//p.entityType="proceeding";
    							proceedingRbc.entityType="proceeding";
    							//assign Proceeding object to Inproceeding object
    							//inproceedingXML.booktitle=p;
    							//add Proceeding object to tempEntities
    							//tempEntities.put(p, "proceeding");
    							tempRbcs.put(proceedingRbc, "proceeding");
    				  			System.out.println("booktitle:"+reader.getText());
    				  			
    				  			// The following should be in inproceedings & END_ELEMENT
    				
    							//assign tempAuthors to Inproceeding object
    							//inproceedingXML.authors=tempAuthors;
    							//add Inproceeding object to tempEntities
    							//tempEntities.put(inproceeding,"inproceeding");
    							tempRbcs.put(inproceedingRbc, "inproceeding");
    							//add tempEntities to entities list of Inproceeding object
    							//inproceedingXML.entities=tempEntities;
    							level=0;
     							// process Relational Neighboors
    							String eType1=""; String eType2="";
    							RelationalBlockedEntityCluster eObject1, eObject2;
    							if (tempRbcs!=null)	{
    								Object[] eRefArray=tempRbcs.entrySet().toArray();
    							
    								for (int i=0;i<eRefArray.length;i++) {
    									eType1=((Entry<Object, String>) eRefArray[i]).getValue();
    									eObject1=(RelationalBlockedEntityCluster)((Entry<Object, String>) eRefArray[i]).getKey();

    									for (int j=0;j<eRefArray.length;j++) {
    										eType2=((Entry<Object, String>) eRefArray[j]).getValue();
    										eObject2=(RelationalBlockedEntityCluster)((Entry<Object, String>) eRefArray[j]).getKey();

    										if(eObject1.globalID!=eObject2.globalID)
    											eObject1.relationalNeighborEntityClusters.put(eObject2, eType2);
    										//context.write(new Text(eObject1.serialize()), new Text(eObject2.serialize()));
    									}
    									//context.write(new Text(String.valueOf(eObject1.globalID)), new Text(eObject1.serialize()));
    									context.write(new Text(String.valueOf("")), new Text(eObject1.serialize()));
    									eObject1.deserialize(eObject1.serialize());
    									System.out.println(eObject1.serialize());
    								}
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
              job.setJarByClass(DBLPInproceedingXMLProcessorOld.class);
              job.setOutputKeyClass(Text.class);
              job.setOutputValueClass(Text.class);

              job.setMapperClass(DBLPInproceedingXMLProcessorOld.Map.class);
              //job.setReducerClass(DBLPInproceedingXMLProcessorNew.Reduce.class);

              job.setInputFormatClass(XmlInputFormat.class);
              job.setOutputFormatClass(TextOutputFormat.class);

              FileSystem fs = FileSystem.get(new Configuration());
              fs.delete(new Path("output"), true);
              
              FileInputFormat.addInputPath(job, new Path("input/xmltestinput.txt"));
              FileOutputFormat.setOutputPath(job, new Path("output"));

              job.waitForCompletion(true);
      }
}

