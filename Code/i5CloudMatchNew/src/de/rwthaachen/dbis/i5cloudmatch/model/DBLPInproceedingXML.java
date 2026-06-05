package de.rwthaachen.dbis.i5cloudmatch.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class DBLPInproceedingXML extends Entity{
	public Date mdate;
	public String key;
	public String title;
	public String pages;
	public int year;
	public List<DBLPAuthor> authors;
	public DBLPProceeding booktitle;	
	public Map<Entity,String> entities;
	public void inproceeding ()	{
		mdate= new Date();
		key="";
		title = "";
		pages= "";
		year = 0;
		authors = new ArrayList <DBLPAuthor>();
		booktitle=null;
	}
}
