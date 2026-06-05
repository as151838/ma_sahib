package de.rwthaachen.dbis.i5cloudmatch.test;

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
	
	static class Attribute {
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
	}
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
	    collection.add(this.blockNeighborEntityClusters);
	    collection.add(this.mergedEntityClusters);
	    String json = gson.toJson(collection);
	    System.out.println("Serilizing: " + json);
	    return json;
  }
  
  public void deserialize(String json) {
		Gson gson = new Gson();
	    JsonParser parser = new JsonParser();
	    JsonArray array = parser.parse(json).getAsJsonArray();
	    //deserialize basic types
	    String globalDataSource = gson.fromJson(array.get(0), String.class);
	    int globalID = gson.fromJson(array.get(1), int.class);
	    String entityType = gson.fromJson(array.get(2), String.class);
	    //deserilizing List<Attribute>
	    Type attributeListType= new TypeToken<List<Attribute>>() {}.getType();
	    List<Attribute> attributes_new= gson.fromJson(array.get(3), attributeListType);
	    //deserizing List<NeighborRBC,String>
	    Type neighborRBCListType= new TypeToken<List<NeighborRBC>>() {}.getType();
	    List<NeighborRBC> relationalNeighborEntityClusters_new= 
	    		gson.fromJson(array.get(4), neighborRBCListType); 
	    System.out.println("Deserializing: ");
	    System.out.println("globalDataSource= "+globalDataSource);
	    System.out.println("globalID= "+globalID);
	    System.out.println("entityType= "+entityType);
	    for (int i=0;i<attributes_new.size();i++)
	    	System.out.println("attribute:"+
	    		"type= "+attributes_new.get(i).type+","+
	    		"name= "+attributes_new.get(i).name+","+
	    		"value= "+attributes_new.get(i).value);
	    for (int i=0;i<relationalNeighborEntityClusters_new.size();i++) {
	    	System.out.print("neighbors:"+
	    		"globalDataSource= "+relationalNeighborEntityClusters_new.get(i).globalDataSource+","+
	    		"globalID= "+relationalNeighborEntityClusters_new.get(i).globalID+","+
	    		"entityType= "+relationalNeighborEntityClusters_new.get(i).entityType);
	    		for (int j=0;i<attributes_new.size();i++)
	    				System.out.print(" attribute: "+
	    						"type= "+relationalNeighborEntityClusters_new.get(i).attributes.get(j).type+","+
	    						"name= "+relationalNeighborEntityClusters_new.get(i).attributes.get(j).name+","+
	    						"value= "+relationalNeighborEntityClusters_new.get(i).attributes.get(j).value);
	    }
	    System.out.println();
  }

  public static void main(String[] args) {
	RBC rbc1=new RBC();
	rbc1.globalDataSource="DBLP";
	rbc1.globalID=1234;
	rbc1.entityType="author";
	rbc1.attributes.add(new Attribute("String", "name", "Sahib"));
	
	NeighborRBC nrbc1= new NeighborRBC();
	nrbc1.globalDataSource="DBLP";
	nrbc1.globalID=231;
	nrbc1.entityType="article";
	nrbc1.attributes.add(new Attribute("String","title","VORBCA"));
	rbc1.relationalNeighborEntityClusters.add(nrbc1);
	String json=rbc1.serialize();

	RBC rbc2=new RBC();
	rbc2.deserialize(json);
  }
}
