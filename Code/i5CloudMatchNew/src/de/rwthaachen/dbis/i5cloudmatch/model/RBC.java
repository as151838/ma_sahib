package de.rwthaachen.dbis.i5cloudmatch.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;

public class RBC {
	public  String globalDataSource;
	public  int globalID;
	public  String entityType;
	public  String clusterLabel;
	public  List<Attribute> attributes= new ArrayList<Attribute>();
	public  List<NeighborRBC> relationalNeighborEntityClusters=
			new ArrayList<NeighborRBC>();
	public List<NeighborRBC> blockNeighborEntityClusters=
			new ArrayList<NeighborRBC>();
	public List<NeighborRBC> mergedEntityClusters=
			new ArrayList<NeighborRBC>();
	
	/*static class Attribute {
		String type;
		String name;
		String value;
		private  Attribute(String type,String name, String value) {
			this.type=type;
			this.name=name;
			this.value=value;
		}
	}
	static class NeighborRBC {
		public  String globalDataSource;
		public  int globalID;
		public  String entityType;
		public  String clusterLabel;
		public  Attribute attribute;
		public  List<Attribute> attributes= new ArrayList<Attribute>();
	}*/
	public void addAttribute(String type,String name, String value) {
		this.attributes.add(new Attribute(type,name,value));
	}
	
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public String serialize() {
	    Gson gson = new Gson();
	    Collection collection = new ArrayList();
	    collection.add(this.globalDataSource);
	    collection.add(this.globalID);
	    collection.add(this.entityType);
	    collection.add(this.attributes);
	    collection.add(this.relationalNeighborEntityClusters);
	    if (!this.blockNeighborEntityClusters.isEmpty())
	    	collection.add(this.blockNeighborEntityClusters);
	    if (!this.mergedEntityClusters.isEmpty())
	    	collection.add(this.mergedEntityClusters);
	    String json = gson.toJson(collection);
	    json=json.replaceAll("\\s", "%%%");
	    System.out.println("Serilizing: json:" + json);
	    System.out.println("Serilizing: size of attributes: " + this.attributes.size());
	    System.out.println("Serilizing: size of relationalNeighborEntityClusters: " + 
	    		this.relationalNeighborEntityClusters.size());
	    return json;
  }
  
  public void deserialize(String json) {
		Gson gson = new Gson();
	    JsonParser parser = new JsonParser();
	    JsonArray array = parser.parse(json).getAsJsonArray();
	    //deserialize basic types
	    this.globalDataSource = gson.fromJson(array.get(0), String.class);
	    this.globalID = gson.fromJson(array.get(1), int.class);
	    this.entityType = gson.fromJson(array.get(2), String.class);
	    //deserilizing List<Attribute>
	    Type attributeListType= new TypeToken<List<Attribute>>() {}.getType();
	    this.attributes= gson.fromJson(array.get(3), attributeListType);
	    //deserizing List<NeighborRBC>
	    Type neighborRBCListType= new TypeToken<List<NeighborRBC>>() {}.getType();
	    this.relationalNeighborEntityClusters= 
	    		gson.fromJson(array.get(4), neighborRBCListType); 
	    System.out.println("Deserializing: "+json);
	    System.out.println("Deserializing: globalDataSource: "+this.globalDataSource);
	    System.out.println("Deserializing: globalID: "+this.globalID);
	    System.out.println("Deserializing: entityType: "+this.entityType);
	    System.out.println("Deserializing: size of attributes:"+this.attributes.size());
	    for (int i=0;i<this.attributes.size();i++)
	    	System.out.println("Deserializing: attribute:"+
	    		"type= "+this.attributes.get(i).type+","+
	    		"name= "+this.attributes.get(i).name+","+
	    		"value= "+this.attributes.get(i).value);
	    
	    System.out.println("Deserializing: size of relationalNeighborEntityClusters:"+
	    	this.relationalNeighborEntityClusters.size());
	    for (int i=0;i<this.relationalNeighborEntityClusters.size();i++) {
	    	System.out.print("Deserializing: neighbors:"+
	    		"globalDataSource= "+this.relationalNeighborEntityClusters.get(i).globalDataSource+","+
	    		"globalID= "+this.relationalNeighborEntityClusters.get(i).globalID+","+
	    		"entityType= "+this.relationalNeighborEntityClusters.get(i).entityType);
	    		for (int j=0;j<this.relationalNeighborEntityClusters.get(i).attributes.size();j++)
	    				System.out.print(" attribute: "+
	    						"type= "+this.relationalNeighborEntityClusters.get(i).attributes.get(j).type+","+
	    						"name= "+this.relationalNeighborEntityClusters.get(i).attributes.get(j).name+","+
	    						"value= "+this.relationalNeighborEntityClusters.get(i).attributes.get(j).value);
	    	System.out.println();
	    }
  }

  public static void main(String[] args) {
	RBC rbc1=new RBC();
	rbc1.globalDataSource="DBLP";
	rbc1.globalID=1234;
	rbc1.entityType="author";
	rbc1.attributes.add(new Attribute("String", "name", "Ammar Sahib"));
	rbc1.attributes.add(new Attribute("String", "university", "RWTH Aachen"));
	
	NeighborRBC nrbc1= new NeighborRBC();
	nrbc1.globalDataSource="DBLP";
	nrbc1.globalID=231;
	nrbc1.entityType="article";
	nrbc1.attributes.add(new Attribute("String","title","VORBCA"));
	nrbc1.attributes.add(new Attribute("int","year","2014"));
	rbc1.relationalNeighborEntityClusters.add(nrbc1);

	NeighborRBC nrbc2= new NeighborRBC();
	nrbc2.globalDataSource="DBLP";
	nrbc2.globalID=103;
	nrbc2.entityType="author";
	nrbc2.attributes.add(new Attribute("String","name","Sami Adil"));
	rbc1.relationalNeighborEntityClusters.add(nrbc2);

	String json=rbc1.serialize();

	RBC rbc2=new RBC();
	rbc2.deserialize("[\"dblp\",2,\"author\",[{\"type\":\"string\",\"name\":\"name\",\"value\":\"Werner%%%John\"}],[{\"globalDataSource\":\"dblp\",\"globalID\":3,\"entityType\":\"author\",\"attributes\":[{\"type\":\"string\",\"name\":\"name\",\"value\":\"Dominik%%%Ley\"}]},{\"globalDataSource\":\"dblp\",\"globalID\":4,\"entityType\":\"author\",\"attributes\":[{\"type\":\"string\",\"name\":\"name\",\"value\":\"Joachim%%%Muller\"}]},{\"globalDataSource\":\"dblp\",\"globalID\":5,\"entityType\":\"proceeding\",\"attributes\":[{\"type\":\"String\",\"name\":\"booktitle\",\"value\":\"(EMC\u002794)\"}]},{\"globalDataSource\":\"dblp\",\"globalID\":1,\"entityType\":\"inproceeding\",\"attributes\":[{\"type\":\"String\",\"name\":\"title\",\"value\":\"ROSAR\"},{\"type\":\"String\",\"name\":\"pages\",\"value\":\"40-43\"},{\"type\":\"int\",\"name\":\"year\",\"value\":\"1994\"}]}]]");
  }
}
