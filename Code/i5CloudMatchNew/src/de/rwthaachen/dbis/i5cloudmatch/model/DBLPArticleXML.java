package de.rwthaachen.dbis.i5cloudmatch.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBLPArticleXML extends Entity{
	public Date mdate;
	public String key;
	public String title;
	public String pages;
	public String month;
	public int year;
	public List<DBLPAuthor> authors;
	public DBLPJournal journal;	
	public Map<Entity,String> entities;
	public void DBLPArticle(){
		mdate= new Date();
		key="";
		title = "";
		pages= "";
		month="";
		year = 0;
		authors = new ArrayList <DBLPAuthor>();
		journal=null;
	}
}
