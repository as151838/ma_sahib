package de.rwthaachen.dbis.i5cloudmatch.controller;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.*;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import org.apache.hadoop.io.Text;
import org.apache.xerces.parsers.DOMParser;
import org.apache.*;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import de.rwthaachen.dbis.i5cloudmatch.model.DBLPArticle;
import de.rwthaachen.dbis.i5cloudmatch.model.DBLPArticleXML;
import de.rwthaachen.dbis.i5cloudmatch.model.DBLPAuthor;
import de.rwthaachen.dbis.i5cloudmatch.model.DBLPJournal;
import de.rwthaachen.dbis.i5cloudmatch.model.Entity;



public class DBLPArticleXMLProcessor extends Parser{
		static String seperator="=";
		String content; 
		DBLPArticleXML articleXML;
		DBLPArticle article;
		BufferedWriter writer; 
		Map<Entity,String> tempEntities=null; 
		List<DBLPAuthor> tempAuthors=null;
		List<String> outputList=new ArrayList<String>();
		int level=0;
		
		public List<String> process (String strXMLContents) {
			parse(strXMLContents);
			processRelationalNeighboors();
			outputList=processRelationalEdges();
			if (tempEntities!=null) System.out.println("DBLPArticleProcessor: processing"+strXMLContents);
			return outputList;
		}
		public void XMLStringtoTempFile (String strXMLContents) {
			try {
				BufferedWriter source = new BufferedWriter(new FileWriter ("temp/DBLPArticle.xml", false));
				source.write(strXMLContents);
				source.close();
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
			
		public void parseXMLFile(String xmlFile) {
		try
			{
				XMLReader xr = XMLReaderFactory.createXMLReader(
					"org.apache.xerces.parsers.SAXParser");
		        xr.setContentHandler(this);
		        xr.setErrorHandler(this);
		        xr.parse(xmlFile);

			}
			catch (SAXException e)
			{
				Logger.getLogger("parser").log(Level.WARNING, 
					"parse.SAXException", e);
			}
			catch (IOException e)
			{
				Logger.getLogger("parser").log(Level.WARNING, 
					"parse.IOException", e);
			}
		}

		public void parse(String xmlContents) {
			try
			{
				writer = new BufferedWriter(new FileWriter("log/smalldblp"));
				XMLStringtoTempFile(xmlContents);
				//parse XML document
				parseXMLFile("temp/DBLPArticle.xml");
				
				writer.close();
			}
			catch (IOException e)
			{
				Logger.getLogger("parser").log(Level.WARNING, 
					"parse.IOException", e);
			}
		}
				
		public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException 
		{
			if (localName.equalsIgnoreCase("article")) {
				level=2;
				//create new Articles object
				articleXML= new DBLPArticleXML();
				article=new DBLPArticle();
				//Assign key and mdate
				articleXML.key=atts.getValue("key");
				try{
					articleXML.mdate= new SimpleDateFormat("yyyy-mm-dd").parse(atts.getValue("mdate"));
				} catch (ParseException e) {
					e.printStackTrace();
				}
				//get new globalID
				article.globalID=GlobalIDGenerator.getGlobalID();
				//Initialize tempAuthors
				tempAuthors=  new ArrayList<DBLPAuthor>();
				//Initialize tempEntities
				tempEntities= new HashMap<Entity,String>();
			}
			
		}
		
		public void endElement(String uri, String localName, String qName) throws SAXException 
		{
			if (level==2 && localName.equalsIgnoreCase("title")) {
				//assign title to article object
				article.title=content;
			}
			if (level==2 && localName.equalsIgnoreCase("year")) {
				//assign year to article object
				article.year=Integer.parseInt(content);
			}
			if (level==2 && localName.equalsIgnoreCase("month")) {
				//assign year to article object
				article.month=content;
			}
			if (level==2 && localName.equalsIgnoreCase("author")) {
				//create new Author object and assign name to Author object
				DBLPAuthor a= new DBLPAuthor(content);
				//get new globalID
				a.globalID=GlobalIDGenerator.getGlobalID();
				//assign entityType and globalDataSource to Author object");
				a.entityType="author";
				a.globalDataSource="dblp";
				//add Author object to tempAuthors
				tempAuthors.add(a);
				//add Author object to tempEntities
				tempEntities.put(a, "author");
			}
			if (level==2 && localName.equalsIgnoreCase("pages")) {
				//assign pages to article object
				article.pages=content;
			}
			if(level==2 && localName.equalsIgnoreCase("journal")) {
				//create new journal object and assign journaltitle to journal object
				DBLPJournal j= new DBLPJournal(content);
				//get new globalID
				j.globalID=GlobalIDGenerator.getGlobalID();
				//assign entityType and globalDataSource to Proceeding object
				j.globalDataSource="dblp";
				j.entityType="journal";
				//assign journal object to article object
				articleXML.journal=j;
				//add journal object to tempEntities
				tempEntities.put(j, "journal");
			}
			if (level==2 && localName.equalsIgnoreCase("article")) {
				//assign entityType and globalDataSource
				article.globalDataSource="dblp";
				article.entityType="article";
				//assign tempAuthors to article object
				articleXML.authors=tempAuthors;
				//add article object to tempEntities
				tempEntities.put(article, "article");
				//add tempEntities to entities list of article object
				articleXML.entities=tempEntities;
				level=0;
			}
		}
		public void characters(char[] ch, int start, int length) throws SAXException {
			content= new String(ch, start, length); 
		}
		public void processRelationalNeighboors() {
			String eType1=""; String eType2="";
			Entity eObject1; Entity eObject2;
			DBLPAuthor author;
			DBLPArticle article;
			DBLPJournal journal;
			
			if (tempEntities!=null)	{
				Object[] eRefArray=tempEntities.entrySet().toArray();
			
				for (int i=0;i<eRefArray.length;i++) {
					eType1=((Entry<Object, String>) eRefArray[i]).getValue();
					eObject1=(Entity)((Entry<Object, String>) eRefArray[i]).getKey();

					for (int j=0;j<eRefArray.length;j++) {
						eType2=((Entry<Object, String>) eRefArray[j]).getValue();
						eObject2=(Entity)((Entry<Object, String>) eRefArray[j]).getKey();

						if(eObject1.globalID!=eObject2.globalID)
							eObject1.relationalNeighborEntites.put(eObject2, eType2);
					}
				}
			}
		}

		public List<String> processRelationalEdges() {
			if (tempEntities==null)	return Collections.EMPTY_LIST;
				String hamaStr="";
				String eType1=""; String eType2="";
				String refStr1=""; String refStr2="";
				Entity eObject1; Entity eObject2;
				DBLPAuthor author;
				DBLPArticle article;
				DBLPJournal journal;
				Object[] eRefArray=tempEntities.entrySet().toArray();
				for (int i=0;i<eRefArray.length;i++) {
					eType1=((Entry<Object, String>) eRefArray[i]).getValue();
					eObject1=(Entity)((Entry<Object, String>) eRefArray[i]).getKey();
					if (eType1=="author") {
						author=(DBLPAuthor)eObject1;
						refStr1=author.serialize();
					}
					if (eType1=="article") {
						article=(DBLPArticle)eObject1;
						refStr1=article.serialize();
					}
					if (eType1=="journal") {
						journal=(DBLPJournal)eObject1;
						refStr1=journal.serialize();
					}
					hamaStr=refStr1;
					for (int j=0;j<eRefArray.length;j++) {
						eType2=((Entry<Object, String>) eRefArray[j]).getValue();
						eObject2=(Entity)((Entry<Object, String>) eRefArray[j]).getKey();
						if (eType2=="author") {
							author=(DBLPAuthor)eObject2;
							refStr2=author.serialize();
						}
						if (eType2=="article") {
							article=(DBLPArticle)eObject2;
							refStr2=article.serialize();
						}
						if (eType2=="proceeding") {
							journal=(DBLPJournal)eObject2;
							refStr2=journal.serialize();
						}

						if(!refStr1.equalsIgnoreCase(refStr2))
							hamaStr=hamaStr+seperator+refStr2;	
					}
					outputList.add(hamaStr);
				}

			return outputList;
		}
}
